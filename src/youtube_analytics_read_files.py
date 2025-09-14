import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ADLS details
STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "youtube-analytics"
STORAGE_KEY = "QIhpMhK6lzlA2trKESzZxW/x7gu13JeMNSrz8ZfpFrVE+dDd2bQ7gU3zylj46Sfr65SF80+0ocWW+ASt5jpSYw=="

def connect_adls():
    conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
    service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = service_client.get_container_client(CONTAINER)
    return container_client

def list_parquet_files(container_client, folder):
    blobs = container_client.list_blobs(name_starts_with=f"{folder}/")
    parquet_files = [blob.name for blob in blobs if blob.name.endswith(".parquet")]
    return parquet_files

def read_parquet_from_adls(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    stream = io.BytesIO(blob_client.download_blob().readall())
    df = pd.read_parquet(stream)
    return df

def summarize_layer(folder, df_dict):
    print(f"\n--- {folder.upper()} LAYER ---")
    if folder == "bronze":
        print("Raw data extracted from Excel, columns normalized. This is the untouched source data for further processing.")
    elif folder == "silver":
        print("Cleaned data with duplicates removed, missing values handled, performance tiers and language detected. Ready for analytics.")
    elif folder == "gold":
        print("Business insights generated: top performers, viral content, traffic source breakdown. This is the data for decision-making.")
    for name, df in df_dict.items():
        print(f"\nFile: {name}")
        print(df.head())
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

def main():
    container_client = connect_adls()
    for folder in ["bronze", "silver", "gold"]:
        files = list_parquet_files(container_client, folder)
        df_dict = {}
        for file in files:
            df = read_parquet_from_adls(container_client, file)
            key = file.split("/")[-1].replace(".parquet", "")
            df_dict[key] = df
        summarize_layer(folder, df_dict)

if __name__ == "__main__":
    main()