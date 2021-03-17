import json
import os
import uuid
import urllib.parse
from metadata import PipelineOperationsClient
from helper import FileHelper, S3Helper, DynamoDBHelper

PIPELINE_STAGE = "DOCUMENT_CLASSIFIER"

metadataTopic  = os.environ.get('METADATA_SNS_TOPIC_ARN', None)
documentTypes  = os.environ.get('documentClassTypes', {
    "NLP_VALID": [
        "internal_research_report",
        "external_public_report"
    ],
    "NLP_INVALID": [
        "classified_report"
    ]
})

print(documentTypes)

if not metadataTopic:
    raise ValueError("Missing arguments.")

pipeline_client = PipelineOperationsClient(metadataTopic)

def processRequest(newImage):
    documentMetadata = newImage.get("documentMetadata")
    documentId = newImage.get("documentId")
    bucketName = newImage.get("bucketName")
    objectName = newImage.get("documentName")
    
    if documentMetadata and documentId and bucketName and objectName:
        print("Valid document item to classify!")
    else:
        raise ValueError("Invalid document item! Please check the incoming dynamoDB record stream")
    
    print("DocumentId: {}, BucketName: {}, ObjectName: {}".format(documentId, bucketName, objectName))
    
    ### This is logic to determine whether or not the document should be sent to NLP processing pipeline
    ### Could be anything; we just determined this could be easy to implement based on document metadata
    print(documentMetadata)
    if documentMetadata['class'] in documentTypes['NLP_VALID']:
        return startNLPProcessing(bucketName, objectName, documentId)
    else:
        return {
            'statusCode': 200,
            'message': "Document {} not eligible for NLP Processing".format(documentId)
        }

def startNLPProcessing(bucketName, objectName, documentId):
    try:
        pipeline_client.body = {
            "documentId": documentId,
            "bucketName": bucketName,
            "objectName": objectName,
            "stage":      PIPELINE_STAGE
        }
        pipeline_client.initDoc()
    except Exception as e:
        print(e)
        pipeline_client.stageFailed("Unable to kick off pipeline")
        
    pipeline_client.stageSucceeded()
    output = "Started NLP for Document {}".format(documentId)
    print(output)
    return {
        'statusCode': 200,
        'message': output
    }

def lambda_handler(event, context):

    print("event: {}".format(event))
    if "Records" in event and event["Records"]:
        for record in event["Records"]:
            try:
                if "eventName" in record and record["eventName"] in ["INSERT", "MODIFY"]:
                    if "dynamodb" in record and record["dynamodb"] and "NewImage" in record["dynamodb"]:
                        print("Processing record: {}".format(record))
                        invokedItem = DynamoDBHelper.deserializeItem(record["dynamodb"]["NewImage"])
                        print(invokedItem)
                        processRequest(invokedItem)
                else:
                    print("Record not an INSERT or MODIFY event in DynamoDB")
            except Exception as e:
                print("Failed to process record. Exception: {}".format(e))
