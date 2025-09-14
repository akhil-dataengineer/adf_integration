import os
from azure.storage.filedatalake import DataLakeServiceClient

def get_adls_client():
    """Connect to Azure Data Lake Storage Gen2 using account key."""
    account_name = os.getenv("ADLS_ACCOUNT_NAME")
    account_key = os.getenv("ADLS_KEY")

    if not account_name or not account_key:
        raise ValueError("⚠️ Missing ADLS credentials. Please set ADLS_ACCOUNT_NAME and ADLS_KEY.")

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )
    return service_client


def list_filesystem_contents(container_name):
    """List files and directories in a given ADLS container."""
    service_client = get_adls_client()
    file_system_client = service_client.get_file_system_client(container_name)

    print(f"📂 Contents of container: {container_name}")
    paths = file_system_client.get_paths()
    for path in paths:
        print(path.name)


def write_to_adls(container_name, file_path, data):
    """Upload text data to ADLS (overwrites if exists)."""
    service_client = get_adls_client()
    file_system_client = service_client.get_file_system_client(container_name)
    file_client = file_system_client.get_file_client(file_path)

    # Ensure directory exists
    dir_path = os.path.dirname(file_path)
    if dir_path:
        try:
            file_system_client.create_directory(dir_path)
        except Exception:
            pass  # Directory may already exist

    file_client.upload_data(data, overwrite=True)
    print(f"✅ Written to {file_path} in {container_name}")


def read_from_adls(container_name, file_path):
    """Read text data from ADLS."""
    service_client = get_adls_client()
    file_system_client = service_client.get_file_system_client(container_name)
    file_client = file_system_client.get_file_client(file_path)

    download = file_client.download_file()
    return download.readall().decode("utf-8")


if __name__ == "__main__":
    container = os.getenv("ADLS_CONTAINER", "social-media-analytics")

    # List all files
    list_filesystem_contents(container)

    # Example write
    write_to_adls(container, "test-folder/hello.txt", "Hello AIDE Agent!")

    # Example read
    content = read_from_adls(container, "test-folder/hello.txt")
    print("📖 File content:", content)
