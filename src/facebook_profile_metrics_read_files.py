import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq

# ADLS details
STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "social-media-analytics"
STORAGE_KEY = "QIhpMhK6lzlA2trKESzZxW/x7gu13JeMNSrz8ZfpFrVE+dDd2bQ7gU3zylj46Sfr65SF80+0ocWW+ASt5jpSYw=="

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

def connect_adls():
    conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
    service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = service_client.get_container_client(CONTAINER)
    return container_client

def read_parquet_from_adls(container_client, folder, name):
    blob_client = container_client.get_blob_client(f"{folder}/{name}.parquet")
    stream = io.BytesIO(blob_client.download_blob().readall())
    table = pq.read_table(stream)
    df = table.to_pandas()
    return df

def read_text_from_adls(container_client, folder, name):
    blob_client = container_client.get_blob_client(f"{folder}/{name}.txt")
    text = blob_client.download_blob().readall().decode()
    return text

def print_transformation_summary(bronze_df, silver_df):
    print("\n=== Transformation Summary ===")
    print(f"Bronze Layer: {bronze_df.shape[0]} rows, {bronze_df.shape[1]} columns")
    print(f"Silver Layer: {silver_df.shape[0]} rows, {silver_df.shape[1]} columns")
    print("\nBronze Columns:")
    print(list(bronze_df.columns))
    print("\nSilver Columns:")
    print(list(silver_df.columns))
    # Show columns added/removed
    added = set(silver_df.columns) - set(bronze_df.columns)
    removed = set(bronze_df.columns) - set(silver_df.columns)
    if added:
        print(f"\nColumns added in Silver: {added}")
    if removed:
        print(f"\nColumns removed in Silver: {removed}")
    # Show sample transformations
    if "performance_tier" in silver_df.columns:
        print("\nPerformance tiers were added based on engagement metrics.")
    if "engagement_per_1k_followers" in silver_df.columns:
        print("Derived metric 'engagement_per_1k_followers' was calculated.")
    print("Empty columns were removed, missing values handled, and duplicates dropped.")

def summarize_gold(container_client):
    print("\n--- Gold Layer ---")
    # Top profiles
    try:
        top_profiles = read_parquet_from_adls(container_client, GOLD, "top_profiles")
        print("Top Performing Profiles:")
        print(top_profiles)
    except Exception:
        print("No top_profiles file found.")
    # Average engagement
    try:
        avg_engagement = read_text_from_adls(container_client, GOLD, "avg_engagement_per_1k")
        print(f"Average Engagement per 1k Followers: {avg_engagement}")
    except Exception:
        print("No avg_engagement_per_1k file found.")
    # Reach trend
    try:
        reach_trend = read_parquet_from_adls(container_client, GOLD, "reach_trend")
        print("Reach Trend Over Time:")
        print(reach_trend.head())
    except Exception:
        print("No reach_trend file found.")
    # Executive summary
    try:
        summary = read_text_from_adls(container_client, GOLD, "executive_summary")
        print("\nExecutive Summary:")
        print(summary)
    except Exception:
        print("No executive_summary file found.")
    print("Gold layer contains business insights and key findings, ready for reporting and decision-making.")

def main():
    container_client = connect_adls()
    # Bronze
    try:
        bronze_df = read_parquet_from_adls(container_client, BRONZE, "profile_metrics_bronze")
        print("\n--- Bronze Layer ---")
        print(bronze_df.head())
    except Exception as e:
        print(f"Bronze layer not found: {e}")
        bronze_df = None
    # Silver
    try:
        silver_df = read_parquet_from_adls(container_client, SILVER, "profile_metrics_silver")
        print("\n--- Silver Layer ---")
        print(silver_df.head())
    except Exception as e:
        print(f"Silver layer not found: {e}")
        silver_df = None
    # Transformation summary
    if bronze_df is not None and silver_df is not None:
        print_transformation_summary(bronze_df, silver_df)
    # Gold
    summarize_gold(container_client)

if __name__ == "__main__":
    main()