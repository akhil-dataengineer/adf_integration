# Databricks notebook: SEMrush Geo Distribution Data Pipeline (ADF Integration, Key Vault, Mock Data)

import logging
import random
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.functions import col

# --- Databricks widgets for ADF parameter integration ---
dbutils.widgets.text("report_month", "2025-08-01")
dbutils.widgets.text("root_folder", "bronze")
dbutils.widgets.text("dbschema", "dev")
dbutils.widgets.text("global_targets_view", "vw_semrush_global_targets")

report_month = dbutils.widgets.get("report_month")
root_folder = dbutils.widgets.get("root_folder")
dbschema = dbutils.widgets.get("dbschema")
global_targets_view = dbutils.widgets.get("global_targets_view")

print(f"Parameters received:")
print(f"  report_month: {report_month}")
print(f"  root_folder: {root_folder}")
print(f"  dbschema: {dbschema}")
print(f"  global_targets_view: {global_targets_view}")

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("semrush_geo_distribution")

# --- Secure Configurations from Key Vault ---
KEYVAULT_SCOPE = "kv-ai-enablement"

def get_secret(name):
    try:
        return dbutils.secrets.get(KEYVAULT_SCOPE, name)
    except Exception as e:
        logger.error(f"Failed to get secret '{name}' from scope '{KEYVAULT_SCOPE}': {e}")
        raise

server = get_secret("sql-server")
database = get_secret("sql-database")
username = get_secret("sql-username")
password = get_secret("sql-password")
adls_key = get_secret("adls-key")

target_query = f"SELECT top 10 clean_url FROM {dbschema}.{global_targets_view}"
display_date = report_month
ADLS_PATH = f"abfss://semrush@aienablementstorage1.dfs.core.windows.net/{root_folder}/geo_distribution/{display_date}"
COUNTRIES = [
    "US", "GB", "DE", "FR", "CA", "AU", "IN", "BR", "JP", "ES", "IT", "NL", "SE", "NO",
    "DK", "PE", "PY", "GH", "HU", "UA", "PA", "BI", "IL", "KR", "PT", "EC"
]
DEVICE_TYPES = ["desktop", "mobile"]

# --- Spark session ---
spark = SparkSession.builder.appName("SEMRushGeoDistribution").getOrCreate()
spark.conf.set("fs.azure.account.key.aienablementstorage1.dfs.core.windows.net", adls_key)
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# --- Read target domains from Azure SQL using provided template ---
logger.info("Reading target domains from Azure SQL Database...")
jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;"
    f"database={database};"
    f"user={username}@sql-opr-adap-dev-eus;"
    f"password={password};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)
try:
    target_df = spark.read \
      .format("com.databricks.spark.sqldw") \
      .option("url", jdbc_url) \
      .option("forwardSparkAzureStorageCredentials", "true") \
      .option("query", target_query) \
      .load()
    target_domains = [row['clean_url'].rstrip('/') for row in target_df.select("clean_url").collect()]
    logger.info(f"Fetched {len(target_domains)} domains from SQL.")
except Exception as e:
    logger.error(f"Failed to read domains: {e}")
    target_domains = []

def generate_mock_geo_data(domain):
    """Generate realistic mock geo distribution data for a domain."""
    global_traffic = random.randint(100000, 5000000)
    device_type = random.choice(DEVICE_TYPES)
    country_shares = random.sample(range(1, 100), len(COUNTRIES))
    total_share = sum(country_shares)
    data = []
    for idx, geo in enumerate(COUNTRIES):
        traffic_share = round(country_shares[idx] / total_share, 4)
        traffic = int(global_traffic * traffic_share)
        users = int(traffic * random.uniform(0.7, 0.95))
        avg_visit_duration = round(random.uniform(30, 300), 2)  # seconds
        bounce_rate = round(random.uniform(0.2, 0.8), 2)
        pages_per_visit = round(random.uniform(1.2, 6.5), 2)
        desktop_share = round(random.uniform(0.3, 0.7), 2) if device_type == "desktop" else round(random.uniform(0.1, 0.4), 2)
        mobile_share = round(1 - desktop_share, 2)
        row = {
            "target_name": domain,
            "display_date": display_date,
            "device_type": device_type,
            "geo": geo,
            "traffic": traffic,
            "global_traffic": global_traffic,
            "traffic_share": traffic_share,
            "users": users,
            "avg_visit_duration": avg_visit_duration,
            "bounce_rate": bounce_rate,
            "pages_per_visit": pages_per_visit,
            "desktop_share": desktop_share,
            "mobile_share": mobile_share,
            "load_timestamp": datetime.datetime.now()
        }
        data.append(row)
    return data

def clean_transform(data):
    """Clean and transform raw geo data."""
    schema = StructType([
        StructField("target_name", StringType(), True),
        StructField("display_date", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("geo", StringType(), True),
        StructField("traffic", IntegerType(), True),
        StructField("global_traffic", IntegerType(), True),
        StructField("traffic_share", DoubleType(), True),
        StructField("users", IntegerType(), True),
        StructField("avg_visit_duration", DoubleType(), True),
        StructField("bounce_rate", DoubleType(), True),
        StructField("pages_per_visit", DoubleType(), True),
        StructField("desktop_share", DoubleType(), True),
        StructField("mobile_share", DoubleType(), True),
        StructField("load_timestamp", TimestampType(), True)
    ])
    df = spark.createDataFrame(data, schema=schema)
    # Data quality checks
    df = df.filter(col("geo").isNotNull() & (col("traffic") > 0))
    null_counts = {col_name: df.filter(col(col_name).isNull()).count() for col_name in df.columns}
    for col_name, count in null_counts.items():
        if count > 0:
            logger.warning(f"Column '{col_name}' has {count} nulls.")
    return df

def save_to_adls(df):
    """Save DataFrame to ADLS as parquet."""
    try:
        df.write.mode("overwrite").parquet(ADLS_PATH)
        logger.info(f"Saved data to {ADLS_PATH}")
    except Exception as e:
        logger.error(f"Error saving to ADLS: {e}")

def main():
    all_data = []
    for domain in target_domains:
        mock_data = generate_mock_geo_data(domain)
        if mock_data:
            all_data.extend(mock_data)
    if not all_data:
        logger.warning("No geo distribution data collected.")
        return
    df = clean_transform(all_data)
    save_to_adls(df)
    # Show sample and summary
    logger.info("Sample data:")
    df.show(5)
    logger.info("Summary statistics:")
    df.describe().show()

if __name__ == "__main__":
    main()