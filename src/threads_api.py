# Databricks Notebook: Scrape Threads Posts with Selenium & Tor Proxy

# Cell 1: Install Chrome and Tor (Linux shell)
# Uncomment and run if needed
# %sh
# apt-get update
# apt-get install -y tor chromium-browser

# Cell 2: Import Libraries
from bs4 import BeautifulSoup
import jmespath
import json
import time
import subprocess
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp
from parsel import Selector

# Nested lookup utility
def nested_lookup(key, document):
    """Recursively find all values of key in document."""
    if isinstance(document, list):
        for d in document:
            for result in nested_lookup(key, d):
                yield result
    elif isinstance(document, dict):
        for k, v in document.items():
            if k == key:
                yield v
            if isinstance(v, (dict, list)):
                for result in nested_lookup(key, v):
                    yield result

# Cell 3: Widgets for SQL view and output path
dbutils.widgets.text("sql_view", "dbo.vw_threads_all_profiles_to_load", "SQL View Name")
dbutils.widgets.text("output_path", "threads/bronze/report_date=" + datetime.today().strftime("%Y-%m-%d"), "Output Path")
sql_view = dbutils.widgets.get("sql_view")
output_path = dbutils.widgets.get("output_path")

# Cell 4: Storage and SQL Configs (replace with your values)
STG_KEY = dbutils.secrets.get("kv-ai-enablement", "adls-key")
STG_ACCOUNT = "stgopradapdeveus"
FINAL_ACCOUNT = "aatdsynapsestg"
CONTAINER = "threads"
SQL_SERVER = "sql-opr-adap-dev-eus.database.windows.net"
SQL_DB = "syndwopradapdeveus"
SQL_USER = dbutils.secrets.get("kv-ai-enablement", "sql-username")
SQL_PASS = dbutils.secrets.get("kv-ai-enablement", "sql-password")
TEMP_DIR = f"wasbs://{CONTAINER}@{STG_ACCOUNT}.blob.core.windows.net/tempDir-threads"

spark.conf.set(f"fs.azure.account.key.{STG_ACCOUNT}.blob.core.windows.net", STG_KEY)
spark.conf.set(f"fs.azure.account.key.{FINAL_ACCOUNT}.dfs.core.windows.net", STG_KEY)

# Cell 5: Get author URLs from SQL view
jdbc_url = f"jdbc:sqlserver://{SQL_SERVER}:1433;database={SQL_DB};user={SQL_USER};password={SQL_PASS};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
profiles_df = spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", jdbc_url) \
    .option("tempDir", TEMP_DIR) \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("query", f"select * from {sql_view}") \
    .load()
author_urls = [row["author_url"] for row in profiles_df.collect()]

# Cell 6: Utility Functions

def restart_tor():
    subprocess.run(["pkill", "tor"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(2)
    subprocess.Popen(["tor"])
    time.sleep(5)

def get_chrome_driver():
    proxy = Proxy()
    proxy.proxy_type = ProxyType.MANUAL
    proxy.socks_proxy = "127.0.0.1:9050"
    proxy.socks_version = 5

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280,1280")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.binary_location = "/usr/bin/chromium-browser"

    capabilities = webdriver.DesiredCapabilities.CHROME.copy()
    proxy.add_to_capabilities(capabilities)

    driver = webdriver.Chrome(options=chrome_options, desired_capabilities=capabilities)
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def scroll_page(driver, max_scrolls=40):
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollBy(0,250);")
        time.sleep(0.5)
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        client_height = driver.execute_script("return window.innerHeight")
        if scroll_height <= client_height:
            break

def extract_thread_json(html):
    sel = Selector(text=html)
    scripts = sel.css('script[type="application/json"][data-sjs]::text').getall()
    for script in scripts:
        try:
            data = json.loads(script)
            if "ScheduledServerJS" in data and "thread_items" in data["ScheduledServerJS"]:
                return data["ScheduledServerJS"]["thread_items"]
        except Exception:
            continue
    return []

def parse_thread_items(thread_items, author_url):
    rows = []
    for post in thread_items:
        post_text = jmespath.search("caption.text", post) or jmespath.search("text", post)
        published_on = jmespath.search("taken_at", post) or jmespath.search("timestamp", post)
        published_on = datetime.fromtimestamp(published_on) if published_on else None
        post_id = jmespath.search("id", post)
        pk = jmespath.search("pk", post)
        code = jmespath.search("code", post)
        username = jmespath.search("user.username", post)
        user_pic = jmespath.search("user.profile_pic_url", post)
        user_verified = jmespath.search("user.is_verified", post)
        user_pk = jmespath.search("user.pk", post)
        user_id = jmespath.search("user.id", post)
        has_audio = jmespath.search("has_audio", post)
        reply_count = jmespath.search("reply_count", post) or jmespath.search("comment_count", post)
        try:
            reply_count = int(str(reply_count).split(" ")[0])
        except Exception:
            reply_count = 0
        like_count = jmespath.search("like_count", post) or 0
        share_count = jmespath.search("share_count", post) or 0
        repost_count = jmespath.search("text_post_app_info.reshare_count", post) or 0
        images = []
        try:
            images = list(set([img.get("url") for img in nested_lookup("image_versions2", post) if isinstance(img, dict) and img.get("candidates") for img in img["candidates"][1:2] if img.get("url")]))
        except Exception:
            images = []
        image_count = len(images)
        videos = list(set(nested_lookup("video_url", post)))
        post_url = f"https://www.threads.net/@{username}/post/{code}" if username and code else None
        rows.append(Row(
            author_url=author_url,
            post_text=post_text,
            published_on=published_on,
            id=post_id,
            pk=pk,
            code=code,
            username=username,
            user_pic=user_pic,
            user_verified=user_verified,
            user_pk=user_pk,
            user_id=user_id,
            has_audio=has_audio,
            reply_count=reply_count,
            like_count=like_count,
            share_count=share_count,
            repost_count=repost_count,
            images=images,
            image_count=image_count,
            videos=videos,
            post_url=post_url,
            report_date=datetime.today().strftime("%Y-%m-%d"),
            load_time=datetime.now()
        ))
    return rows

# Cell 7: Define Schema
schema = StructType([
    StructField("author_url", StringType()),
    StructField("post_text", StringType()),
    StructField("published_on", TimestampType()),
    StructField("id", StringType()),
    StructField("pk", StringType()),
    StructField("code", StringType()),
    StructField("username", StringType()),
    StructField("user_pic", StringType()),
    StructField("user_verified", BooleanType()),
    StructField("user_pk", StringType()),
    StructField("user_id", StringType()),
    StructField("has_audio", BooleanType()),
    StructField("reply_count", IntegerType()),
    StructField("like_count", IntegerType()),
    StructField("share_count", IntegerType()),
    StructField("repost_count", IntegerType()),
    StructField("images", ArrayType(StringType())),
    StructField("image_count", IntegerType()),
    StructField("videos", ArrayType(StringType())),
    StructField("post_url", StringType()),
    StructField("report_date", StringType()),
    StructField("load_time", TimestampType())
])

# Cell 8: Scraping Loop with Retry Logic
results = []
for idx, author_url in enumerate(author_urls):
    print(f"Scraping {idx+1}/{len(author_urls)}: {author_url}")
    for attempt in range(3):
        try:
            restart_tor()
            driver = get_chrome_driver()
            driver.get(author_url)
            scroll_page(driver)
            html = driver.page_source
            thread_items = extract_thread_json(html)
            rows = parse_thread_items(thread_items, author_url)
            results.extend(rows)
            driver.quit()
            break
        except Exception as e:
            print(f"Error scraping {author_url} (attempt {attempt+1}): {e}")
            time.sleep(15)
            continue

# Cell 9: Convert to DataFrame, Save to ADLS, Display
df = spark.createDataFrame(results, schema=schema)
df = df.withColumn("published_on", to_timestamp(col("published_on"))) \
       .withColumn("report_date", lit(datetime.today().strftime("%Y-%m-%d"))) \
       .withColumn("load_time", current_timestamp())

adls_path = f"abfss://{CONTAINER}@{FINAL_ACCOUNT}.dfs.core.windows.net/{output_path}"
df.coalesce(2).write.format('parquet').mode('overwrite').save(adls_path)
print(f"Data written to: {adls_path}")