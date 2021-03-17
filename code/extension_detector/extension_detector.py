import json
import os
from helper import FileHelper, AwsHelper, S3Helper
from metadata import DocumentLineageClient, PipelineOperationsClient

PIPELINE_STAGE = "EXTENSION_DETECTOR"

syncBucketName = os.environ.get('TARGET_SYNC_BUCKET', None)
asyncBucketName = os.environ.get('TARGET_ASYNC_BUCKET', None)
metadataTopic  = os.environ.get('METADATA_SNS_TOPIC_ARN', None)

if not syncBucketName or not asyncBucketName or not metadataTopic:
    raise Exception("Missing lambda environment variables")
    
pipeline_client = PipelineOperationsClient(metadataTopic)
lineage_client = DocumentLineageClient(metadataTopic)

def processRequest(documentId, bucketName, objectName, callerId):

    output = ""
    pipeline_client.body = {
        "documentId": documentId,
        "bucketName": bucketName,
        "objectName": objectName,
        "stage":      PIPELINE_STAGE
    }
    pipeline_client.stageInProgress()
    print("Input Object: {}/{}".format(bucketName, objectName))

    ext = FileHelper.getFileExtension(objectName.lower())
    print("Extension: {}".format(ext))

    if(ext and ext in ["jpg", "jpeg", "png"]):
        targetBucketName = syncBucketName
    elif (ext in ["pdf"]):
        targetBucketName = asyncBucketName
    else:
        raise Exception("Incorrect file extension")
    targetFileName = "{}/{}".format(documentId, objectName)  
    if(targetBucketName):
        print("Doing S3 Object Copy for documentId: {}, object: {}/{}".format(documentId, targetBucketName, targetFileName))
        try:
            S3Helper().copyToS3(bucketName, objectName, targetBucketName, targetFileName)
        except Exception as e:
           print(e)
           pipeline_client.stageFailed()
    else:
        print("")
        pipeline_client.stageFailed()

    output = "Completed S3 Object Copy for documentId: {}, object: {}/{}".format(documentId, targetBucketName, targetFileName)
    lineage_client.recordLineageOfCopy({
        "documentId":       documentId,
        "callerId":         callerId,
        "sourceBucketName": bucketName,
        "targetBucketName": targetBucketName,
        "sourceFileName":   objectName,
        "targetFileName":   targetFileName,
    })
    pipeline_client.stageSucceeded()
    print(output)

def processRecord(record, syncBucketName, asyncBucketName, callerId):
    
    newImage = record["dynamodb"]["NewImage"]
    
    documentId = None
    bucketName = None
    objectName = None
    
    if("documentId" in newImage and "S" in newImage["documentId"]):
        documentId = newImage["documentId"]["S"]
    if("bucketName" in newImage and "S" in newImage["bucketName"]):
        bucketName = newImage["bucketName"]["S"]
    if("objectName" in newImage and "S" in newImage["objectName"]):
        objectName = newImage["objectName"]["S"]

    print("DocumentId: {}, BucketName: {}, ObjectName: {}".format(documentId, bucketName, objectName))

    if(documentId and bucketName and objectName):
        processRequest(documentId, bucketName, objectName, callerId)

def lambda_handler(event, context):
    callerId = context.invoked_function_arn
    print(callerId)
    try:
        
        print("event: {}".format(event))

        if("Records" in event and event["Records"]):
            for record in event["Records"]:
                try:
                    print("Processing record: {}".format(record))

                    if("eventName" in record and record["eventName"] == "INSERT"):
                        if("dynamodb" in record and record["dynamodb"] and "NewImage" in record["dynamodb"]):
                            processRecord(record, syncBucketName, asyncBucketName, callerId)

                except Exception as e:
                    print("Failed to process record. Exception: {}".format(e))

    except Exception as e:
        print("Failed to process records. Exception: {}".format(e))