import sys
import re
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Dynamic Key Vault Scope Detection ---
def detect_scope():
    try:
        scopes = dbutils.secrets.listScopes()
        available_scopes = [s.name for s in scopes]
        print(f"Available secret scopes: {available_scopes}")
        for scope in available_scopes:
            try:
                _ = dbutils.secrets.get(scope, "adls-key")
                print(f"Using scope: {scope}")
                return scope
            except Exception as e:
                print(f"Scope '{scope}' does not have required secrets: {e}")
        print("No valid scope found, defaulting to 'kv-ai-enablement'")
        return "kv-ai-enablement"
    except Exception as e:
        print(f"Error listing scopes: {e}. Defaulting to 'kv-ai-enablement'")
        return "kv-ai-enablement"

KEYVAULT_SCOPE = detect_scope()

def get_secret(name):
    try:
        return dbutils.secrets.get(KEYVAULT_SCOPE, name)
    except Exception as e:
        print(f"Failed to get secret '{name}' from scope '{KEYVAULT_SCOPE}': {e}")
        raise

# --- ADLS Config ---
STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "social-media-analytics"
adls_key = get_secret("adls-key")

spark = SparkSession.builder.appName("SocialMediaMedallionPipeline").getOrCreate()
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", adls_key)

CSV_PATH = "/dbfs/FileStore/tables/emplifi_profile_metrics.csv"
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/profile_metrics"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/profile_metrics"
LOGS_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/logs/pipeline_log"

BATCH_ID = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

def log_to_adls(spark, message, log_path=LOGS_PATH):
    log_df = spark.createDataFrame([(datetime.now().isoformat(), message)], ["timestamp", "message"])
    log_df.write.mode("append").parquet(log_path)

def normalize_columns(df):
    # Lowercase, snake_case
    new_cols = [re.sub(r'\W+', '_', c.strip().lower()) for c in df.columns]
    for old, new in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old, new)
    return df

def preview_data(df, name="Data Preview"):
    print(f"\n=== {name} ===")
    df.printSchema()
    df.show(5, truncate=False)
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    print("Null counts per column:", null_counts)
    print("Row count:", df.count())

def bronze_layer(spark, df):
    try:
        df = normalize_columns(df)
        df = df.withColumn("load_timestamp", F.lit(datetime.utcnow().isoformat()))
        df = df.withColumn("source_file", F.lit(CSV_PATH))
        df = df.withColumn("batch_id", F.lit(BATCH_ID))
        preview_data(df, "Bronze Layer Preview")
        df.write.mode("overwrite").parquet(BRONZE_PATH)
        log_to_adls(spark, f"Bronze layer: {df.count()} rows ingested. Columns: {df.columns}")
        return df
    except Exception as e:
        log_to_adls(spark, f"Bronze layer error: {e}")
        print(f"Bronze layer error: {e}")
        sys.exit(1)

def silver_layer(spark, df):
    try:
        # Remove duplicates
        df = df.dropDuplicates()
        # Engagement score calculation
        engagement_cols = [c for c in df.columns if any(x in c for x in ["like", "comment", "reaction", "share"])]
        if engagement_cols:
            score_expr = " + ".join([f"coalesce({c},0)" for c in engagement_cols])
            df = df.withColumn("engagement_score", F.expr(score_expr))
            # Quantile-based performance tier
            window = Window.orderBy(F.desc("engagement_score"))
            df = df.withColumn("rank", F.row_number().over(window))
            total = df.count()
            df = df.withColumn("performance_tier",
                F.when(F.col("rank") <= total/3, "high")
                 .when(F.col("rank") <= 2*total/3, "medium")
                 .otherwise("low")
            ).drop("rank")
        else:
            df = df.withColumn("engagement_score", F.lit(None)).withColumn("performance_tier", F.lit("unknown"))
        preview_data(df, "Silver Layer Preview")
        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
        duplicate_count = df.count() - df.dropDuplicates().count()
        log_to_adls(spark, f"Silver layer: {df.count()} rows after cleaning. Null %: {null_counts}, Duplicates: {duplicate_count}")
        df.write.mode("overwrite").parquet(SILVER_PATH)
        return df
    except Exception as e:
        log_to_adls(spark, f"Silver layer error: {e}")
        print(f"Silver layer error: {e}")
        sys.exit(1)

def compare_layers(spark):
    try:
        bronze_df = spark.read.parquet(BRONZE_PATH)
        silver_df = spark.read.parquet(SILVER_PATH)
        print("\n=== Layer Comparison ===")
        print(f"Bronze rows: {bronze_df.count()}, Silver rows: {silver_df.count()}")
        print("Bronze columns:", bronze_df.columns)
        print("Silver columns:", silver_df.columns)
        bronze_df.show(3, truncate=False)
        silver_df.show(3, truncate=False)
        return bronze_df, silver_df
    except Exception as e:
        log_to_adls(spark, f"Layer comparison error: {e}")
        print(f"Layer comparison error: {e}")

def executive_summary(silver_df):
    print("\n=== Executive Summary ===")
    if "performance_tier" in silver_df.columns and "engagement_score" in silver_df.columns:
        top = silver_df.filter(F.col("performance_tier") == "high")
        avg_score = silver_df.select(F.avg("engagement_score")).collect()[0][0]
        print(f"Top performers (high tier):")
        if "profile_name" in silver_df.columns:
            top_names = [r["profile_name"] for r in top.select("profile_name").distinct().collect()]
            print(", ".join(top_names))
        print(f"Average engagement score: {avg_score:.2f}")
        print("Engagement score distribution:")
        silver_df.select("engagement_score").describe().show()
    else:
        print("No engagement score or performance tier found.")

def main():
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)
    except Exception as e:
        log_to_adls(spark, f"Error reading CSV: {e}")
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    bronze_df = bronze_layer(spark, df)
    silver_df = silver_layer(spark, bronze_df)
    bronze_df, silver_df = compare_layers(spark)
    executive_summary(silver_df)

if __name__ == "__main__":
    main()