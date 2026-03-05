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
date_partition = 'test_date_value'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get source folder path

# CELL ********************

current_workspace = notebookutils.runtime.context.get("currentWorkspaceName")
lakehouses = mssparkutils.lakehouse.list()
source_folder  = ([x for x in lakehouses if x.get('displayName') == target_lakehouse][0].
    get('properties').get('abfsPath') + f'/Files/bronze/{project}/landing/{date_partition}/')
print('Source folder= ' + [x for x in lakehouses if x.get('displayName') == target_lakehouse][0].
  get('properties').get('abfsPath'))
# mssparkutils.fs.mount('abfss://55adfdd1-632d-4ac7-9568-ab2ced451e96@onelake.dfs.fabric.microsoft.com/86590cc3-50a7-4464-b869-a51cc1b27e1b', "<mountPoint>")
target_table = ([x for x in lakehouses if x.get('displayName') == target_lakehouse][0].
    get('properties').get('abfsPath') + f'/Tables/bronze/{project}' )
# target = f"`bronze`.`{project}`" 
# # print(lakehouses)
# print(source_folder)
print('Target table =' + target_table)

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
    option("multiline", "true").json(folder + '*.json').
    withColumn('source_file', F.input_file_name())
)
df = df_raw.withColumn('insert_timestamp',F.current_timestamp())
# df = df.withColumn('country', F.regexp_replace(df.source_file,f'{source_folder}(\d+).json',''))

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
    save(target_table)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
