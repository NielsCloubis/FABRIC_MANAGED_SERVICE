# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6a8f1ff8-4dd0-489f-804f-f17df16383ae",
# META       "default_lakehouse_name": "lh_monitoring",
# META       "default_lakehouse_workspace_id": "53a11cf1-b2a0-4a78-88d1-0fb0f78d3ac2",
# META       "known_lakehouses": [
# META         {
# META           "id": "6a8f1ff8-4dd0-489f-804f-f17df16383ae"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Get logs from Fabric Monitoring API

# CELL ********************

from datetime import datetime, timedelta
import hashlib
import json
import requests
import sys
from pyspark.sql.functions import col
from azure.identity import ClientSecretCredential

# hardcoded eruithalen
TENANT_ID = "a082dbbc-d89d-4018-b336-7279f22177eb"
CLIENT_ID = "60f23a80-14ee-486f-9ea3-4516183afd2d"

# Key Vault
KV_NAME = "kv-sandbox-fabric"
KV_URI  = f"https://{KV_NAME}.vault.azure.net/"
KV_SECRET_NAME = "fabric-managed-service-client-secret"

# =========================
# GET SECRET FROM KEY VAULT
# =========================
try:
    CLIENT_SECRET = notebookutils.credentials.getSecret(KV_URI, KV_SECRET_NAME)
    if not CLIENT_SECRET:
        raise RuntimeError("Empty secret retrieved from Key Vault.")
except Exception as ex:
    raise RuntimeError(
        f"Failed to read secret '{KV_SECRET_NAME}' from Key Vault '{KV_URI}'. "
        f"Ensure the running user has secret-get permissions on the vault."
    ) from ex

# =========================
# AUTHENTICATE TO FABRIC (Service Principal)
# =========================
SCOPE = "https://api.fabric.microsoft.com/.default"

cred = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)
token = cred.get_token(SCOPE).token

HEADERS = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}


def record_hash(record):
    fields = [
        "datetime_t", "artifact_type_s", "artifact_name_s", "artifact_id_s", "run_id_s",
        "run_url_s", "workspace_id_g", "user_s", "environment_s",
        "priority_s", "workspace_name_s", "message_type_s", "short_message_s", "Message"
    ]
    stable_record = {k: (record.get(k) or "").strip() for k in fields}

    if stable_record["datetime_t"]:
        dt = datetime.fromisoformat(stable_record["datetime_t"].replace("Z", "")[:19])
        stable_record["datetime_t"] = dt.isoformat() + "Z"

    record_str = json.dumps(stable_record, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(record_str.encode("utf-8")).hexdigest()

def get_artifact_priority_from_tags(workspace_id, item_id, token):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        tags = data.get("tags", [])
        if tags:
            return tags[0].get("displayName")
        else:
            return "None"
    except Exception as e:
        print(f"Error to obtain the tags for artifact_id: {item_id}: {e}")
        return "None"
    
def map_pipeline_log(item, token):
    workspace_id = item.get("workspaceObjectId")
    artifact_id = item.get("artifactObjectId")
    art_type = item.get("artifactType", "").strip()
    status = item.get("statusString", "").strip()
    artifact_name = item.get("artifactName", "unknown")
    workspace_name = item.get("workspaceName", "").strip()
    invoke_type = item.get("ArtifactJobInvokeTypeString", "Unknown")

    if "dev" in workspace_name.lower():
        environment = "dev"
    elif "acc" in workspace_name.lower():
        environment = "acc"
    else:
        environment = "prd"

    message_type = "ERROR" if status == "Failed" else "INFO"

    if art_type == "Pipeline":
        short_message_map = {
            "Failed": "Pipeline Failed",
            "Cancelled": "Pipeline Cancelled",
            "Completed": "Pipeline Completed"
        }
        short_message = short_message_map.get(status, "Pipeline Status Unknown")

    elif art_type == "dataset":
        short_message_map = {
            "Failed": "Dataset Refresh Failed",
            "Cancelled": "Dataset Refresh Cancelled",
            "Completed": "Dataset Refresh Completed"
        }
        short_message = short_message_map.get(status, "Dataset Refresh Status Unknown")

    elif art_type == "DataflowFabric":
        short_message_map = {
            "Failed": "DataFlow Run Failed",
            "Cancelled": "Dataflow Run Cancelled",
            "Completed": "Dataflow Run Completed"
        }
        short_message = short_message_map.get(status, "Dataflow Run Failed")

    else:
        short_message = "Unknown"

    service_exception = item.get("serviceExceptionJson", "")

    if art_type == "Pipeline":
        if status == "Failed":
            try:
                err_data = json.loads(service_exception)
                err_msg = err_data.get("ErrorMessage", "Unknown failure")
            except:
                err_msg = "Unknown failure"
            message_detail = f"Error to run the pipeline {artifact_name}: {err_msg}"
        elif status == "Completed":
            message_detail = f"Pipeline {artifact_name} run successfully"
        elif status == "Cancelled":
            message_detail = f"Pipeline {artifact_name} was cancelled"
        else:
            message_detail = f"Pipeline {artifact_name} has unknown status"

    elif art_type.lower() == "dataset":
        if status == "Failed":
            try:
                err_data = json.loads(service_exception)
                details = err_data["error"]["pbi.error"]["details"]
                value_msg = next(
                    (d["detail"]["value"] for d in details if "detail" in d and "value" in d["detail"]),
                    "Unknown failure"
                )
                err_msg = value_msg
            except:
                err_msg = "Unknown failure"
            message_detail = f"Error to refresh dataset {artifact_name}: {err_msg}"

        elif status == "Completed":
            message_detail = f"Dataset {artifact_name} refresh succeeded"

        elif status == "Cancelled":
            message_detail = f"Dataset {artifact_name} refresh was cancelled"

        else:
            message_detail = f"Dataset {artifact_name} has unknown status"

    elif art_type == "DataflowFabric":
        if status == "Failed":
            try:
                err_data = json.loads(service_exception)
                err_msg = err_data.get("ErrorMessage", "Unknown failure")
            except:
                err_msg = "Unknown failure"
            message_detail = f"Error to run the dataflow {artifact_name}: {err_msg}"
        elif status == "Completed":
            message_detail = f"Dataflow {artifact_name} run successfully"
        elif status == "Cancelled":
            message_detail = f"Dataflow {artifact_name} was cancelled"
        else:
            message_detail = f"Dataflow {artifact_name} has unknown status"

    else:
        message_detail = f"{artifact_name} unknown type"

    full_message = f"Trigger Event: {invoke_type} - {message_detail}"

    priority = get_artifact_priority_from_tags(workspace_id, artifact_id, token)

    job_end_time_str = item.get("jobEndTimeUtc")
    job_end_time = datetime.fromisoformat(job_end_time_str[:26])
    job_end_time_brussels = job_end_time + timedelta(hours=2)
    datetime_t = job_end_time_brussels.isoformat()

    if art_type.lower() == "pipeline":
        run_url = (
            f"https://app.powerbi.com/workloads/data-pipeline/monitoring/workspaces/"
            f"{item.get('workspaceObjectId')}/pipelines/"
            f"{item.get('artifactObjectId')}/"
            f"{item.get('artifactJobInstanceId')}?experience=power-bi"
        )
    else:
        run_url = "None"

    return {
        "datetime_t": datetime_t,
        "artifact_type_s": item.get("artifactType"),
        "artifact_name_s": artifact_name,
        "artifact_id_s": item.get("artifactObjectId"),
        "run_id_s": item.get("artifactJobInstanceId"),
        "run_url_s": run_url,
        "priority_s": priority,
        "workspace_id_g": item.get("workspaceObjectId"),
        "user_s": (item.get("ownerUser") or {}).get("userPrincipalName"),
        "environment_s": environment,
        "workspace_name_s": workspace_name,
        "message_type_s": message_type,
        "short_message_s": short_message,
        "Message": full_message
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import sys
# sys.path.append("/lakehouse/default/Files/libraries/fabric_logs")
# from fabric_logs import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------
try:
    import notebookutils
except ImportError:
    from notebookutils import mssparkutils as notebookutils 

from azure.identity import ClientSecretCredential
from pyspark.sql.functions import col

TENANT_ID = "a082dbbc-d89d-4018-b336-7279f22177eb"         
CLIENT_ID = "60f23a80-14ee-486f-9ea3-4516183afd2d"           
KV_URI    = "https://kv-sandbox-fabric.vault.azure.net/"      
KV_SECRET = "fabric-managed-service-client-secret"           

CLIENT_SECRET = notebookutils.credentials.getSecret(KV_URI, KV_SECRET)  
cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

fabric_token = cred.get_token("https://api.fabric.microsoft.com/.default").token
# ------------------------------------------------------------



now = datetime.utcnow()
twenty_five_hours_ago = now - timedelta(hours=25)

df_existing = (
    spark.read.table("dbo.fabric_logs")
        .filter(col("datetime_t") >= twenty_five_hours_ago)
)

existing_records = [row.asDict() for row in df_existing.collect()]
existing_hashes = {record_hash(r) for r in existing_records}

# print(f"✔ {len(existing_hashes)} existing records loaded (last 25h).")

token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {token}"}

start_time = (now - timedelta(hours=25)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
end_time = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

params = [
    ("limit", 20000),
    ("startTime", start_time),
    ("endTime", end_time),
    ("artifactTypes", "Pipeline,dataset,DataflowFabric"),
    ("Accept", "application/json"),
]

response = requests.get(
    "https://wabi-west-europe-d-primary-redirect.analysis.windows.net/metadata/monitoringhub/histories",
    headers=headers,
    params=params
)
response.raise_for_status()

data = response.json()
logs = data["logs"] if "logs" in data else data

allowed_status = {"Completed", "Failed", "Cancelled"}
logs = [item for item in logs if item.get("statusString", "").strip() in allowed_status]

mapped_logs = [map_pipeline_log(item, fabric_token) for item in logs]

unique_new_logs = [
    log for log in mapped_logs
    if record_hash(log) not in existing_hashes
]

print(f"🆕 New logs found: {len(unique_new_logs)}")

if unique_new_logs:
    df_new = spark.createDataFrame(unique_new_logs)
    df_new.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("dbo.fabric_logs")
    # df_new.write.format("delta").partitionBy('run_id_s').mode("overwrite").option("mergeSchema", "true").saveAsTable("dbo.fabric_logs")
    print("✅ New logs inserted successfully!")
else:
    print("ℹ No new logs to insert.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
