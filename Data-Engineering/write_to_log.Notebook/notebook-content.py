# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType, LongType, DoubleType, ArrayType, IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
api_call_info = """
{
    "variableName": "run_id",
    "value": {
        "PipelineName": "4296cede-d288-43e1-9a7d-30cc5d60bf11",
        "PipelineRunId": "15aceba3-f0c7-4fbc-94a5-c2c5b38e7eb9",
        "JobId": "15aceba3-f0c7-4fbc-94a5-c2c5b38e7eb9",
        "ActivityRunId": "13622bda-c337-4484-9771-75cbbbd7733b",
        "ExecutionStartTime": "2026-02-18T16:16:40.9097105Z",
        "ExecutionEndTime": "2026-02-18T16:17:01.4869177Z",
        "Status": "Failed",
        "Error": {
            "errorCode": "2200",
            "message": "ErrorCode=RestCallFailedWithClientError,'Type=Microsoft.DataTransfer.Common.Shared.HybridDeliveryException,Message=Rest call failed with client error, status code 404 NotFound, please check your activity settings.\nRequest URL: https://restcountries.com/v3.1/name/Antigua%20&%20Barbuda.\nResponse: {\"_links\":{\"self\":[{\"href\":\"/v3.1/name/Antigua%20&%20Barbuda\",\"templated\":false}]},\"_embedded\":{\"errors\":[{\"message\":\"Page Not Found\"}]},\"message\":\"Not Found\"},Source=Microsoft.DataTransfer.ClientLibrary,'",
            "failureType": "UserError",
            "target": "Response to json",
            "details": []
        },
        "Output": {
            "copyDuration": 15,
            "errors": [
                {
                    "Code": 23353,
                    "Message": "ErrorCode=RestCallFailedWithClientError,'Type=Microsoft.DataTransfer.Common.Shared.HybridDeliveryException,Message=Rest call failed with client error, status code 404 NotFound, please check your activity settings.\nRequest URL: https://restcountries.com/v3.1/name/Antigua%20&%20Barbuda.\nResponse: {\"_links\":{\"self\":[{\"href\":\"/v3.1/name/Antigua%20&%20Barbuda\",\"templated\":false}]},\"_embedded\":{\"errors\":[{\"message\":\"Page Not Found\"}]},\"message\":\"Not Found\"},Source=Microsoft.DataTransfer.ClientLibrary,'",
                    "EventType": 0,
                    "Category": 5,
                    "Data": {},
                    "MsgId": null,
                    "ExceptionType": null,
                    "Source": null,
                    "StackTrace": null,
                    "InnerEventInfos": []
                }
            ],
            "effectiveIntegrationRuntime": "WorkspaceIR (West Europe)",
            "usedDataIntegrationUnits": 4,
            "billingReference": {
                "activityType": "DataMovement",
                "billableDuration": [
                    {
                        "meterType": "AzureIR",
                        "duration": 0.016666666666666666,
                        "unit": "DIUHours"
                    }
                ],
                "totalBillableDuration": [
                    {
                        "meterType": "AzureIR",
                        "duration": 0.016666666666666666,
                        "unit": "DIUHours"
                    }
                ]
            },
            "usedParallelCopies": 1,
            "executionDetails": [
                {
                    "source": {
                        "type": "RestService"
                    },
                    "sink": {
                        "type": "Lakehouse"
                    },
                    "status": "Failed",
                    "start": "2026-02-18T16:16:44.3810564Z",
                    "duration": 15,
                    "usedDataIntegrationUnits": 4,
                    "usedParallelCopies": 1,
                    "profile": {
                        "queue": {
                            "status": "Completed",
                            "duration": 10
                        },
                        "transfer": {
                            "status": "Completed",
                            "duration": 3
                        }
                    },
                    "detailedDurations": {
                        "queuingDuration": 10,
                        "transferDuration": 3
                    }
                }
            ],
            "dataConsistencyVerification": {
                "VerificationResult": "Unsupported"
            },
            "durationInQueue": {
                "integrationRuntimeQueue": 0
            }
        },
        "ExecutionDetails": {
            "integrationRuntime": [
                {
                    "name": "WorkspaceIR",
                    "type": "Managed",
                    "location": "West Europe",
                    "nodes": null
                }
            ]
        },
        "StatusCode": 400,
        "ExecutionStatus": "Fail",
        "Duration": "00:00:20.5772072",
        "RecoveryStatus": "None",
        "ActivityType": "Copy"
    }
}
"""
copy_data = r"""
{
    "variableName": "log",
    "value": "{\"workspace_id\":\"55adfdd1-632d-4ac7-9568-ab2ced451e96\",\"pipeline_id\":\"4296cede-d288-43e1-9a7d-30cc5d60bf11\",\"run_id\":\"278a3a7b-a5aa-4957-9bb9-b610b9c8eb79\",\"output\":{\"dataRead\":4381,\"dataWritten\":3287,\"filesWritten\":1,\"sourcePeakConnections\":1,\"sinkPeakConnections\":1,\"rowsRead\":1,\"rowsCopied\":1,\"copyDuration\":23,\"throughput\":0.548,\"errors\":[],\"effectiveIntegrationRuntime\":\"WorkspaceIR (West Europe)\",\"usedDataIntegrationUnits\":4,\"billingReference\":{\"activityType\":\"DataMovement\",\"billableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.016666666666666666,\"unit\":\"DIUHours\"}],\"totalBillableDuration\":[{\"meterType\":\"AzureIR\",\"duration\":0.016666666666666666,\"unit\":\"DIUHours\"}]},\"usedParallelCopies\":1,\"executionDetails\":[{\"source\":{\"type\":\"RestService\"},\"sink\":{\"type\":\"Lakehouse\"},\"status\":\"Succeeded\",\"start\":\"2026-02-20T15:57:13.9177723Z\",\"duration\":23,\"usedDataIntegrationUnits\":4,\"usedParallelCopies\":1,\"profile\":{\"queue\":{\"status\":\"Completed\",\"duration\":13},\"transfer\":{\"status\":\"Completed\",\"duration\":8,\"details\":{\"readingFromSource\":{\"type\":\"RestService\",\"workingDuration\":0,\"timeToFirstByte\":0},\"writingToSink\":{\"type\":\"Lakehouse\",\"workingDuration\":3}}}},\"detailedDurations\":{\"queuingDuration\":13,\"timeToFirstByte\":0,\"transferDuration\":8}}],\"dataConsistencyVerification\":{\"VerificationResult\":\"NotVerified\"},\"durationInQueue\":{\"integrationRuntimeQueue\":0}}}"
}
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(data=[(copy_data,)], schema=["raw"])

schema_raw = StructType(
    [
        StructField("variableName", StringType()),
        StructField("value", StringType())
    ]
    )
log = json.loads(df.withColumn("json", F.from_json(F.col("raw"), schema=schema_raw)).select( F.col("json.value")).first()[0])
# data = df.withColumn("json", F.from_json(F.col("raw"), schema=schema_raw)).select( F.col("json.value"))
display(log)

# df = spark.createDataFrame(data=[log])
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
