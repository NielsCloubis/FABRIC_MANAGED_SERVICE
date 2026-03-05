# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "af255abb-96ff-4559-9852-c31278b39b1f",
# META       "default_lakehouse_name": "lh_monitoring",
# META       "default_lakehouse_workspace_id": "55adfdd1-632d-4ac7-9568-ab2ced451e96",
# META       "known_lakehouses": [
# META         {
# META           "id": "af255abb-96ff-4559-9852-c31278b39b1f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import sys
sys.path.append("/lakehouse/default/Files/libraries/fabric_logs")
from fabric_logs import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

mapped_logs = [map_pipeline_log(item, token) for item in logs]

unique_new_logs = [
    log for log in mapped_logs
    if record_hash(log) not in existing_hashes
]

print(f"🆕 New logs found: {len(unique_new_logs)}")

if unique_new_logs:
    df_new = spark.createDataFrame(unique_new_logs)
    df_new.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("dbo.fabric_logs")
    # df_new.write.format("delta").partitionBy('run_id_s').mode("overwrite").option("mergeSchema", "true").saveAsTable("dbo.fabric_logs")
    print("✅ New logs inserted successfully!")
else:
    print("ℹ No new logs to insert.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
