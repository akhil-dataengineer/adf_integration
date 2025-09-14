import os
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# ---------------------------
# Setup: fetch secrets from Key Vault (with local fallback)
# ---------------------------
KEYVAULT_NAME = os.getenv("KEYVAULT_NAME")  # e.g. "kv-ai-enablement"
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")  # e.g. "https://adb-xxx.azuredatabricks.net"

if not KEYVAULT_NAME or not DATABRICKS_HOST:
    raise ValueError("Missing environment variables: KEYVAULT_NAME or DATABRICKS_HOST")

# First check if DATABRICKS_TOKEN is already in env (for quick local testing)
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_TOKEN:
    # Fallback: fetch from Key Vault using DefaultAzureCredential
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=f"https://{KEYVAULT_NAME}.vault.azure.net/", credential=credential)
    DATABRICKS_TOKEN = secret_client.get_secret("databricks-pat").value


# ---------------------------
# Databricks run wrapper
# ---------------------------
def databricks_run(notebook_path: str, params: dict = None, existing_cluster_id: str = None):
    """
    Submit a Databricks notebook job with parameters.

    Args:
        notebook_path (str): Full workspace path to the notebook (e.g., '/Repos/etl/bronze_ingest').
        params (dict): Dictionary of parameters to pass to the notebook.
        existing_cluster_id (str, optional): If provided, will use an existing cluster instead of creating a new one.
    """
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    task_config = {
        "task_key": "agent_task",
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": params or {}
        }
    }

    # Use existing cluster OR new ephemeral cluster
    if existing_cluster_id:
        task_config["existing_cluster_id"] = existing_cluster_id
    else:
        task_config["new_cluster"] = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 2
        }

    payload = {
        "run_name": f"AIDE Agent Run - {notebook_path}",
        "tasks": [task_config]
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    run_id = response.json()["run_id"]
    print(f"✅ Databricks job submitted: Run ID = {run_id}")
    return run_id


# ---------------------------
# Test locally
# ---------------------------
if __name__ == "__main__":
    # Example: run with ephemeral cluster
    run1 = databricks_run(
        notebook_path="/Repos/demo/bronze_ingest",
        params={"report_date": "2025-09-11"}
    )
    print("Triggered Run ID (ephemeral):", run1)

    # Example: run with existing cluster
    # run2 = databricks_run(
    #     notebook_path="/Repos/demo/bronze_ingest",
    #     params={"report_date": "2025-09-11"},
    #     existing_cluster_id="0909-203659-hpz7xu9m"
    # )
    # print("Triggered Run ID (existing cluster):", run2)
