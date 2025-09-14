import os
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# ---------------------------
# Setup: fetch secrets from Key Vault (with local fallback)
# ---------------------------
KEYVAULT_NAME = os.getenv("KEYVAULT_NAME")  # e.g. "kv-ai-enablement"
ADF_SUBSCRIPTION_ID = os.getenv("ADF_SUBSCRIPTION_ID")
ADF_RESOURCE_GROUP = os.getenv("ADF_RESOURCE_GROUP")
ADF_FACTORY_NAME = os.getenv("ADF_FACTORY_NAME")

if not (KEYVAULT_NAME and ADF_SUBSCRIPTION_ID and ADF_RESOURCE_GROUP and ADF_FACTORY_NAME):
    raise ValueError("Missing env vars: KEYVAULT_NAME, ADF_SUBSCRIPTION_ID, ADF_RESOURCE_GROUP, ADF_FACTORY_NAME")

# Get AAD token for ARM management
credential = DefaultAzureCredential()
token = credential.get_token("https://management.azure.com/.default").token


# ---------------------------
# ADF pipeline upsert wrapper
# ---------------------------
def adf_upsert_pipeline(pipeline_name: str, pipeline_json: dict):
    """
    Create or update an ADF pipeline using REST API.

    Args:
        pipeline_name (str): The name of the pipeline to create/update.
        pipeline_json (dict): Pipeline definition JSON (datasets, linked services, activities).
    """
    url = (
        f"https://management.azure.com/subscriptions/{ADF_SUBSCRIPTION_ID}/"
        f"resourceGroups/{ADF_RESOURCE_GROUP}/providers/Microsoft.DataFactory/"
        f"factories/{ADF_FACTORY_NAME}/pipelines/{pipeline_name}?api-version=2018-06-01"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.put(url, headers=headers, json=pipeline_json)
    response.raise_for_status()

    print(f"✅ Pipeline '{pipeline_name}' created/updated successfully in ADF.")
    return response.json()


# ---------------------------
# Test locally
# ---------------------------
if __name__ == "__main__":
    # Example: minimal pipeline JSON (replace with your own full definition)
    sample_pipeline = {
        "properties": {
            "activities": [
                {
                    "name": "Wait1",
                    "type": "Wait",
                    "typeProperties": {"waitTimeInSeconds": 5}
                }
            ]
        }
    }

    result = adf_upsert_pipeline("agent_test_pipeline", sample_pipeline)
    print("Pipeline result:", result)
