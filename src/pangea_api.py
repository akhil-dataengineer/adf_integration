# Databricks Notebook Cell 1: Imports & Secure Configuration
import aiohttp
import asyncio
import time
import json
from datetime import datetime, timedelta
from enum import Enum
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import *
from bs4 import BeautifulSoup
from tqdm import tqdm

# Secure secrets from Azure Key Vault
ADLS_KEY = dbutils.secrets.get(scope="kv-ai-enablement", key="adls-key")
PANGEA_API_KEY = dbutils.secrets.get(scope="kv-ai-enablement", key="pangea-api-key")
SQL_USERNAME = dbutils.secrets.get(scope="kv-ai-enablement", key="sql-username")
SQL_PASSWORD = dbutils.secrets.get(scope="kv-ai-enablement", key="sql-password")

STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "pangea"
SQL_SERVER = "sql-opr-adap-dev-eus.database.windows.net"
SQL_DB = "syndwopradapdeveus"

# Databricks Notebook Cell 2: Base Class
class ADLSAPIBase:
    def __init__(self, storage_account, container, adls_key, sql_server, sql_db, sql_username, sql_password):
        self.storage_account = storage_account
        self.container = container
        self.adls_key = adls_key
        self.sql_server = sql_server
        self.sql_db = sql_db
        self.sql_username = sql_username
        self.sql_password = sql_password
        self.set_adls_keys_2_spark_config()

    def set_adls_keys_2_spark_config(self):
        spark.conf.set(
            f"fs.azure.account.key.{self.storage_account}.dfs.core.windows.net",
            self.adls_key
        )

    def get_accounts(self, usagm_entities):
        entities = [e.strip().strip("'") for e in usagm_entities.split(",")]
        entities_str = ",".join([f"'{e}'" for e in entities])
        query = f"""
            (SELECT url, entity, source_language, source_language_code, auto_detected
             FROM vw_opr_analytics_dictionary_account_urls
             WHERE platform_opr='Adobe Analytics' AND entity IN ({entities_str}))
        """
        jdbc_url = f"jdbc:sqlserver://{self.sql_server}:1433;database={self.sql_db};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", self.sql_username) \
            .option("password", self.sql_password) \
            .option("dbtable", query) \
            .load()
        return df.collect()

    def export_to_adls(self, df: DataFrame, path: str):
        df.coalesce(1).write.mode("overwrite").parquet(path)

# Databricks Notebook Cell 3: Error Classes
class RetryableError(Exception): pass
class IgnorableError(Exception): pass

# Databricks Notebook Cell 4: Main API Class
class Periodicity(Enum):
    DAY = 'daily'
    MONTH = 'monthly'

class PangeaApi(ADLSAPIBase):
    def __init__(self, api_key, from_date, to_date, periodicity, path_prefix, usagm_entities,
                 storage_account, container, adls_key, sql_server, sql_db, sql_username, sql_password):
        super().__init__(storage_account, container, adls_key, sql_server, sql_db, sql_username, sql_password)
        self.api_key = api_key
        self.from_date = from_date
        self.to_date = to_date
        self.periodicity = Periodicity(periodicity)
        self.path_prefix = path_prefix
        self.usagm_entities = usagm_entities

    def headers(self):
        # Pangea expects session cookies, not Bearer tokens
        return {
            "Cookie": f"pangea_session={self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def build_urls(self, accounts):
        urls = []
        date_range = [self.from_date + timedelta(days=x) for x in range((self.to_date - self.from_date).days + 1)]
        for acc in accounts:
            base_url = acc.url
            for date in date_range:
                urls.append({
                    "url": f"{base_url}/api2/articles?dateFrom={date.strftime('%Y-%m-%d')}&dateTo={date.strftime('%Y-%m-%d')}",
                    "entity": acc.entity,
                    "source_language": acc.source_language,
                    "source_language_code": acc.source_language_code,
                    "auto_detected": acc.auto_detected,
                    "date_from": date.strftime('%Y-%m-%d'),
                    "date_to": date.strftime('%Y-%m-%d')
                })
        return urls

    async def fetch_article(self, session, url_obj, semaphore, retries=3):
        async with semaphore:
            url = url_obj["url"]
            for attempt in range(retries):
                try:
                    async with session.get(url, headers=self.headers(), timeout=30) as resp:
                        if resp.status in [401, 403]:
                            raise IgnorableError(f"Auth error {resp.status}")
                        if resp.status >= 500:
                            raise RetryableError(f"Server error {resp.status}")
                        if resp.status in [400, 404]:
                            return []
                        if resp.headers.get("Content-Type", "").startswith("application/json"):
                            data = await resp.json()
                            articles = data.get("articles", [])
                            if len(articles) > 1000:
                                articles = articles[:1000]
                            # Attach metadata from account
                            for art in articles:
                                art["entity"] = url_obj["entity"]
                                art["source_language"] = url_obj["source_language"]
                                art["source_language_code"] = url_obj["source_language_code"]
                                art["auto_detected"] = url_obj["auto_detected"]
                                art["date_from"] = url_obj["date_from"]
                                art["date_to"] = url_obj["date_to"]
                            return articles
                        else:
                            raise RetryableError("Non-JSON response")
                except RetryableError:
                    await asyncio.sleep(2 ** attempt)
                    continue
                except Exception as e:
                    raise e
            return []

    async def extract_articles_async(self, urls):
        semaphore = asyncio.Semaphore(4)
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_article(session, url_obj, semaphore) for url_obj in urls]
            results = []
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Extracting articles"):
                result = await f
                if result: results.extend(result)
            return results

    def schema(self):
        return StructType([
            StructField("id", StringType()),
            StructField("title", StringType()),
            StructField("introduction", StringType()),
            StructField("pubDate", TimestampType()),
            StructField("contentType", StringType()),
            StructField("url", StringType()),
            StructField("zone", StringType()),
            StructField("authors", ArrayType(StringType())),
            StructField("distributionChannels", ArrayType(StringType())),
            StructField("url_parameter", StringType()),
            StructField("source_language", StringType()),
            StructField("source_language_code", StringType()),
            StructField("auto_detected", StringType()),
            StructField("entity", StringType()),
            StructField("date_from", StringType()),
            StructField("date_to", StringType()),
            StructField("content", StringType()),
            StructField("load_time", TimestampType())
        ])

    def html_to_text(self, html):
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator=" ", strip=True)

    def transform(self, articles):
        rows = []
        for art in articles:
            if not art or not art.get("id"): continue
            rows.append(Row(
                id=art.get("id"),
                title=art.get("title"),
                introduction=self.html_to_text(art.get("introduction")),
                pubDate=datetime.strptime(art.get("pubDate"), "%Y-%m-%dT%H:%M:%SZ") if art.get("pubDate") else None,
                contentType=art.get("contentType"),
                url=art.get("url"),
                zone=art.get("zone"),
                authors=art.get("authors", []),
                distributionChannels=art.get("distributionChannels", []),
                url_parameter=art.get("url_parameter"),
                source_language=art.get("source_language"),
                source_language_code=art.get("source_language_code"),
                auto_detected=art.get("auto_detected"),
                entity=art.get("entity"),
                date_from=art.get("date_from"),
                date_to=art.get("date_to"),
                content=self.html_to_text(art.get("content")),
                load_time=datetime.now()
            ))
        return rows

    def export(self, df: DataFrame):
        if self.periodicity == Periodicity.MONTH:
            partition = self.from_date.strftime("%Y-%m")
        else:
            partition = self.from_date.strftime("%Y-%m-%d")
        path = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{self.path_prefix}/report_date={partition}/"
        self.export_to_adls(df, path)