# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F
import notebookutils
from datetime import datetime as dt


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_lakehouse = 'lh_test_data_test_value'
source = 'rest_countries_test_value'
source_type = 'test'
date_partition = 'test_date_value'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get paths

# CELL ********************

current_workspace = notebookutils.runtime.context.get("currentWorkspaceName")
lakehouses = mssparkutils.lakehouse.list()

source_path  = ([x for x in lakehouses if x.get('displayName') == target_lakehouse][0].
    get('properties').get('abfsPath') + f'/Files/{source_type}/{source}/landing/{date_partition}/')
print('Source path =' + source_path)

target_path = ([x for x in lakehouses if x.get('displayName') == target_lakehouse][0].
    get('properties').get('abfsPath') + f'/Tables/{source}')
print('Target path =' + target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create dataframe

# CELL ********************

df_raw = (
    spark.read.
    option("multiline", "true").json(source_path + '*.json').
    withColumn('source_file', F.input_file_name())
)
df = df_raw.withColumn('insert_timestamp',F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to delta table

# CELL ********************

(
    df.write.format('delta').
    mode('overwrite').
    option('overwriteSchema', 'true').
    save(target_path)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
