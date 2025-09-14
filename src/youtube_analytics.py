import io
import pandas as pd
import matplotlib.pyplot as plt
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import pyarrow as pa

# ADLS details
STORAGE_ACCOUNT = "aienablementstorage1"
CONTAINER = "youtube-analytics"
STORAGE_KEY = "QIhpMhK6lzlA2trKESzZxW/x7gu13JeMNSrz8ZfpFrVE+dDd2bQ7gU3zylj46Sfr65SF80+0ocWW+ASt5jpSYw=="
FILE_NAME = "input/YouTubeImpressionsMar19Mar24.xlsx"

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

def log(msg):
    print(f"[LOG] {msg}")

def connect_adls():
    conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
    service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = service_client.get_container_client(CONTAINER)
    log("Connected to ADLS.")
    return container_client

def ensure_folders(container_client):
    for folder in [BRONZE, SILVER, GOLD]:
        try:
            container_client.get_blob_client(f"{folder}/.keep").upload_blob(b"", overwrite=True)
            log(f"Ensured folder: {folder}")
        except Exception as e:
            log(f"Folder {folder} may already exist: {e}")

def download_excel(container_client):
    blob_client = container_client.get_blob_client(FILE_NAME)
    stream = io.BytesIO(blob_client.download_blob().readall())
    log("Downloaded Excel file from ADLS.")
    return stream

def save_parquet_to_adls(container_client, df, folder, name):
    table = pa.Table.from_pandas(df)
    out = io.BytesIO()
    pq.write_table(table, out)
    out.seek(0)
    blob_client = container_client.get_blob_client(f"{folder}/{name}.parquet")
    blob_client.upload_blob(out, overwrite=True)
    log(f"Saved {name} to {folder} as Parquet.")

def normalize_columns(df):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def bronze_layer(container_client, excel_stream):
    xls = pd.ExcelFile(excel_stream)
    sheets = xls.sheet_names
    bronze_data = {}
    log(f"Found sheets: {sheets}")
    for sheet in sheets:
        df = pd.read_excel(xls, sheet_name=sheet)
        df = normalize_columns(df)
        log(f"Sheet '{sheet}' columns: {list(df.columns)}")
        save_parquet_to_adls(container_client, df, BRONZE, sheet.strip().lower().replace(" ", "_"))
        bronze_data[sheet.strip().lower().replace(" ", "_")] = df
        log(f"Bronze: {sheet} shape {df.shape}")
    return bronze_data

def data_quality_checks(df, name):
    issues = []
    if df.isnull().sum().sum() > 0:
        issues.append("Missing values detected.")
    if df.duplicated().sum() > 0:
        issues.append("Duplicate rows detected.")
    log(f"Data Quality [{name}]: {'; '.join(issues) if issues else 'OK'}")
    return issues

def detect_language(text):
    if pd.isnull(text):
        return "unknown"
    for c in str(text):
        if '\u0600' <= c <= '\u06FF':
            return "arabic"
    return "english"

def silver_layer(container_client, bronze_data):
    silver_data = {}
    for name, df in bronze_data.items():
        df = df.copy()
        df = df.drop_duplicates()
        df = df.fillna("unknown")
        # Add performance tiers if impressions/views column exists
        perf_col = None
        for col in df.columns:
            if "impressions" in col:
                perf_col = col
                break
            if "views" in col:
                perf_col = col
        if perf_col:
            df["performance_tier"] = pd.qcut(df[perf_col].astype(float), 3, labels=["low", "medium", "high"])
        # Detect language for title/content columns
        lang_col = None
        for col in df.columns:
            if "title" in col:
                lang_col = col
                break
            if "content" in col:
                lang_col = col
        if lang_col:
            df["language"] = df[lang_col].apply(detect_language)
        data_quality_checks(df, name)
        save_parquet_to_adls(container_client, df, SILVER, name)
        silver_data[name] = df
        log(f"Silver: {name} shape {df.shape}")
    return silver_data

def gold_layer(container_client, silver_data):
    insights = {}
    # Top performers and viral content
    for name, df in silver_data.items():
        perf_col = None
        lang_col = None
        for col in df.columns:
            if "impressions" in col:
                perf_col = col
            if "views" in col and not perf_col:
                perf_col = col
            if "title" in col:
                lang_col = col
            if "content" in col and not lang_col:
                lang_col = col
        if perf_col and lang_col and "performance_tier" in df.columns:
            top = df.sort_values(by=perf_col, ascending=False).head(5)
            insights["top_performers"] = top[[lang_col, perf_col, "performance_tier", "language"]]
            viral = df[df["performance_tier"] == "high"]
            insights["viral_content"] = viral[[lang_col, perf_col, "language"]]
            log(f"Gold: Top performers and viral content extracted from {name}.")
            break
    # Traffic source breakdown
    for name, df in silver_data.items():
        if "source" in df.columns and "views" in df.columns:
            breakdown = df.groupby("source")["views"].sum().reset_index()
            insights["traffic_source_breakdown"] = breakdown
            log(f"Gold: Traffic source breakdown extracted from {name}.")
            break
    # Save insights
    for key, df in insights.items():
        save_parquet_to_adls(container_client, df, GOLD, key)
    return insights

def generate_summary(insights):
    summary = []
    if "top_performers" in insights:
        top_titles = insights["top_performers"].iloc[:,0].tolist()
        summary.append(f"Top 5 performing items: {', '.join(map(str, top_titles))}")
    if "viral_content" in insights:
        viral_count = len(insights["viral_content"])
        summary.append(f"{viral_count} items classified as viral (high tier).")
    if "traffic_source_breakdown" in insights:
        sources = insights["traffic_source_breakdown"]["source"].tolist()
        summary.append(f"Main traffic sources: {', '.join(map(str, sources))}")
    log("Executive Summary:")
    for line in summary:
        print(f"  - {line}")
    return summary

def plot_insights(insights):
    # Top performers bar chart
    if "top_performers" in insights:
        df = insights["top_performers"]
        plt.figure(figsize=(8,4))
        plt.bar(df.iloc[:,0], df.iloc[:,1])
        plt.title("Top 5 by Impressions/Views")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("/tmp/top_performers.png")
        log("Chart saved: top_performers.png")
    # Traffic source pie chart
    if "traffic_source_breakdown" in insights:
        df = insights["traffic_source_breakdown"]
        plt.figure(figsize=(6,6))
        plt.pie(df["views"], labels=df["source"], autopct='%1.1f%%')
        plt.title("Traffic Source Breakdown")
        plt.savefig("/tmp/traffic_sources.png")
        log("Chart saved: traffic_sources.png")

def main():
    container_client = connect_adls()
    ensure_folders(container_client)
    excel_stream = download_excel(container_client)
    bronze_data = bronze_layer(container_client, excel_stream)
    silver_data = silver_layer(container_client, bronze_data)
    insights = gold_layer(container_client, silver_data)
    generate_summary(insights)
    plot_insights(insights)
    log("Pipeline complete.")

if __name__ == "__main__":
    main()