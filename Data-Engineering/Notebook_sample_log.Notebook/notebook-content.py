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

# log_lakehouse = 'lh_monitoring'
# log_table = 'fabric_logs'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# --- CONFIG: change these to match your environment ---
LAKEHOUSE_TABLE = "lh_monitoring.dbo.fabric_logs"   # Delta table with your logs
SAMPLE_ROWS     = 100                       # how many sample rows for the portal
OUTPUT_PATH     = "Files/samples/Logs_sample.json"  # appears under Lakehouse > Files

from pyspark.sql.functions import col, coalesce, to_timestamp, date_format, current_timestamp, lit

# 1) Read your Delta table
df = spark.table(LAKEHOUSE_TABLE)
# display(df)

# print(df.columns)

# 2) Choose/rename the columns you want in Log Analytics.
#    Make sure there's a TimeGenerated column in ISO 8601 (UTC). If you have an event time, use it; otherwise use now().
#    Replace 'event_time','level','message','service','userId','latency_ms' with your actual column names.
df_shaped = (
    df.withColumn(
        "TimeGenerated",
        date_format(
            current_timestamp(),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        )
    )
    .select("TimeGenerated", "datetime_t",  'artifact_type_s', 'artifact_name_s', 'artifact_id_s', 'artifact_id_s', 'run_url_s', 'workspace_id_g', 'user_s', 'environment_s', 'workspace_name_s', 'message_type_s', 'short_message_s', 'Message', 'priority_s')
    .limit(SAMPLE_ROWS)
)

# display(df_shaped)
# 3) Turn the sample into a JSON array string (what the portal expects for schema inference)
json_records = [r for r in df_shaped.toJSON().toLocalIterator()]
json_array_str = "[\n" + ",\n".join(json_records) + "\n]"

# 4) Write the JSON file into the Lakehouse Files area so you can download it
#    In Fabric notebooks, mssparkutils is available for file I/O in the 'Files' mount.
from notebookutils import mssparkutils
mssparkutils.fs.mkdirs("Files/samples")
mssparkutils.fs.put(OUTPUT_PATH, json_array_str, overwrite=True)

print(f"Wrote sample file to: {OUTPUT_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
