import boto3
from decimal import Decimal
import json
import os
import urllib.parse
from helper import AwsHelper, S3Helper, DynamoDBHelper
from metadata import PipelineOperationsClient, DocumentLineageClient
from og import OutputGenerator

PIPELINE_STAGE = "SYNC_PROCESS_TEXTRACT"

textractBucketName = os.environ.get("TARGET_TEXTRACT_BUCKET_NAME", None)
metadataTopic  = os.environ.get('METADATA_SNS_TOPIC_ARN', None)

if not textractBucketName or not metadataTopic:
    raise ValueError("Missing arguments.")

pipeline_client = PipelineOperationsClient(metadataTopic)
lineage_client = DocumentLineageClient(metadataTopic)

def callTextract(bucketName, objectName):
    textract = AwsHelper().getClient('textract')
    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucketName,
                'Name': objectName
            }
        }
    )
    return response


def processImage(documentId, bucketName, objectName, callerId):

    response = callTextract(bucketName, objectName)

    print("Generating output for documentId: {}".format(documentId))

    opg = OutputGenerator(
        documentId = documentId,
        response   = response,
        bucketName = textractBucketName,
        objectName = objectName,
        forms      = False,
        tables     = False
    )
    tagging = "documentId={}".format(documentId)
    opg.writeTextractOutputs(taggingStr=tagging)
    
    lineage_client.recordLineage({
        "documentId":       documentId,
        "callerId":         callerId,
        "sourceBucketName": bucketName,
        "targetBucketName": textractBucketName,
        "sourceFileName":   objectName,
        "targetFileName":   objectName
    })

# --------------- Main handler ------------------

def processRequest(bucketName, objectName, callerId):

    output = ""

    documentId = S3Helper().getTagsS3(bucketName, objectName).get('documentId', None)
    if not documentId:
        raise Exception("Unidentified document. Please check its tags.")
    
    pipeline_client.body = {
        "documentId": documentId,
        "bucketName": bucketName,
        "objectName": objectName,
        "stage":      PIPELINE_STAGE
    }
    pipeline_client.stageInProgress()
   
    print('Task ID: ' + documentId)

    if(documentId and bucketName and objectName):
        print("DocumentId: {}, Object: {}/{}".format(documentId, bucketName, objectName))

        processImage(documentId, bucketName, objectName, callerId)

        output = "Document: {}, Object: {}/{} processed.".format(documentId, bucketName, objectName)
        pipeline_client.stageSucceeded()
        print(output)
    else:
        pipeline_client.stageFailed()
        
    return {
        'statusCode': 200,
        'body': output
    }

def lambda_handler(event, context):

    print("Sync Processor event: {}".format(event))
    
    bucketName = event['Records'][0]['s3']['bucket']['name']
    objectName = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    callerId   = context.invoked_function_arn
    return processRequest(bucketName, objectName, callerId)