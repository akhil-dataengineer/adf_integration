# Databricks Notebook Cell 1: Widgets & Imports
dbutils.widgets.text("year_month", "2024-11-19")
dbutils.widgets.text("path_prefix", "bronze")
dbutils.widgets.dropdown("periodicity", "monthly", ["monthly", "daily"])
dbutils.widgets.text("usagm_entities", "'VOA','RFE','OCB'")

from datetime import datetime, timedelta

# %run "./pangea_api"

# Databricks Notebook Cell 2: Parse Inputs & Credentials
year_month = dbutils.widgets.get("year_month")
path_prefix = dbutils.widgets.get("path_prefix")
periodicity = dbutils.widgets.get("periodicity")
usagm_entities = dbutils.widgets.get("usagm_entities")

ADLS_KEY = dbutils.secrets.get(scope="kv-ai-enablement", key="adls-key")
PANGEA_API_KEY = dbutils.secrets.get(scope="kv-ai-enablement", key="pangea-api-key")
SQL_USERNAME = dbutils.secrets.get(scope="kv-ai-enablement", key="sql-username")
SQL_PASSWORD = dbutils.secrets.get(scope="kv-ai-enablement", key="sql-password")
STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "pangea"
SQL_SERVER = "sql-opr-adap-dev-eus.database.windows.net"
SQL_DB = "syndwopradapdeveus"

from_date = datetime.strptime(year_month, "%Y-%m-%d")
to_date = from_date + timedelta(days=1 if periodicity == "daily" else 30)

# Databricks Notebook Cell 3: Initialize API Wrapper
pangea_api = PangeaApi(
    api_key=PANGEA_API_KEY,
    from_date=from_date,
    to_date=to_date,
    periodicity=periodicity,
    path_prefix=path_prefix,
    usagm_entities=usagm_entities,
    storage_account=STORAGE_ACCOUNT,
    container=CONTAINER,
    adls_key=ADLS_KEY,
    sql_server=SQL_SERVER,
    sql_db=SQL_DB,
    sql_username=SQL_USERNAME,
    sql_password=SQL_PASSWORD
)

# Databricks Notebook Cell 4: Main Extraction & Export
accounts = pangea_api.get_accounts(usagm_entities)
urls = pangea_api.build_urls(accounts)
articles = asyncio.run(pangea_api.extract_articles_async(urls))
rows = pangea_api.transform(articles)
df = spark.createDataFrame(rows, schema=pangea_api.schema())

pangea_api.export(df)

# Databricks Notebook Cell 5: Quality Assurance & Record Count
def count_check_api_adls(api_count, adls_count):
    print(f"API count: {api_count}, ADLS count: {adls_count}")
    return api_count == adls_count

api_count = len([a for a in articles if a and a.get("id")])
adls_count = df.count()
qc_passed = count_check_api_adls(api_count, adls_count)

# Update tracking table
if qc_passed:
    spark.sql(f"DELETE FROM usagmoprds.usagmoprdsdb.records_count_in_layers WHERE report_date='{year_month}' AND layer='{path_prefix}'")
    spark.sql(f"""
        INSERT INTO usagmoprds.usagmoprdsdb.records_count_in_layers (report_date, layer, api_count, adls_count, load_time)
        VALUES ('{year_month}', '{path_prefix}', {api_count}, {adls_count}, '{datetime.now()}')
    """)

# Databricks Notebook Cell 6: Display DataFrame
display(df)