# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aefc087a-25ac-4995-b383-d077cb3304f0",
# META       "default_lakehouse_name": "FUAM_Lakehouse",
# META       "default_lakehouse_workspace_id": "53a11cf1-b2a0-4a78-88d1-0fb0f78d3ac2",
# META       "known_lakehouses": [
# META         {
# META           "id": "aefc087a-25ac-4995-b383-d077cb3304f0"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Create Snapshot Tables
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**Delete, if day exists, then Append**</mark>
# 
# ##### Related pipeline:
# 
# ****
# 
# ##### Source:
# 
# ** Selected Tables from Lakehouse** in FUAM_Lakehouse
# 
# ##### Target:
# ** History Tables from Lakehouse** in FUAM_Lakehouse
# 
# 
# 


# CELL ********************

import requests
from pyspark.sql.functions import col, lit, udf, explode, to_date, json_tuple, from_json, schema_of_json, get_json_object, concat
from pyspark.sql.types import StringType, json
from pyspark.sql import SparkSession
import json
from delta.tables import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## Parameters
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
tables = [
    {'name' :'active_items' , 'snapshot_id_cols' : ['id', 'capacityId', 'workspaceId']},
    {'name' :'capacities'  , 'snapshot_id_cols' : ['CapacityId']},
    {'name' :'workspaces'  , 'snapshot_id_cols' : ['CapacityId', 'WorkspaceId']},
    {'name' :'workspaces_scanned_users'  , 'snapshot_id_cols' : ['WorkspaceId']}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in tables:

    table_name = table['name']
    ids = table['snapshot_id_cols']
    history_table = table_name + '_history'

    print(history_table)
    curr_date = datetime.datetime.now().strftime('%Y-%m-%d')

    df = spark.sql(f"SELECT * FROM {table_name}" )
    df = df.withColumn("Snapshot_Date", to_date(lit(curr_date)))
    for id in ids:
        df = df.withColumn("Snapshot_" + id , concat(lit(curr_date),lit("_"), col(id))) 

    if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', history_table ):

        print("table exists")
        sql = """ DELETE FROM """ + history_table + """
        WHERE Snapshot_Date = '""" + curr_date + """'  
        """

        spark.sql(sql)
        df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(history_table)

    else:
        print("History table will be created.")

        df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(history_table)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
