import json
import boto3
import os
import urllib.parse
from helper import AwsHelper, S3Helper
import time
from metadata import PipelineOperationsClient

PIPELINE_STAGE = "ASYNC_START_TEXTRACT"

snsTopic       = os.environ.get('TEXTRACT_SNS_TOPIC_ARN', None)
snsRole        = os.environ.get('TEXTRACT_SNS_ROLE_ARN', None)
metadataTopic  = os.environ.get('METADATA_SNS_TOPIC_ARN', None)

if not snsTopic or not snsRole or not metadataTopic:
    raise ValueError("Missing arguments.")

pipeline_client = PipelineOperationsClient(metadataTopic)

def startJob(bucketName, objectName, documentId, snsTopic, snsRole):
    print("Starting job with documentId: {}, bucketName: {}, objectName: {}".format(documentId, bucketName, objectName))

    response = None
    client = AwsHelper().getClient('textract')

    response = client.start_document_analysis(
        ClientRequestToken  = documentId,
        DocumentLocation={
            'S3Object': {
                'Bucket': bucketName,
                'Name': objectName
            }
        },
        FeatureTypes=["FORMS", "TABLES"],
        NotificationChannel= {
              "RoleArn": snsRole,
              "SNSTopicArn": snsTopic
        },
        JobTag = documentId)
    return response["JobId"]


def processItem(bucketName, objectName, snsTopic, snsRole):
    print('Bucket Name: ' + bucketName)
    print('Object Name: ' + objectName)
    
    documentId = S3Helper().getTagsS3(bucketName, objectName).get('documentId', None)
    if not documentId:
        raise Exception("Unidentified document. Please check its tags.")
        
    print('Task ID: ' + documentId)

    pipeline_client.body = {
        "documentId": documentId,
        "bucketName": bucketName,
        "objectName": objectName,
        "stage":      PIPELINE_STAGE
    }
    pipeline_client.stageInProgress()
    jobId = startJob(bucketName, objectName, documentId, snsTopic, snsRole)

    if (jobId):
        pipeline_client.stageSucceeded()
        print("Started Job with Id: {}".format(jobId))
    else:
        pipeline_client.stageFailed()
    return jobId

def lambda_handler(event, context):
    if 's3' in event['Records'][0]:
        print("Async Processor event: {}".format(event))
        bucketName = event['Records'][0]['s3']['bucket']['name']
        objectName = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        
        return processItem(bucketName, objectName, snsTopic, snsRole)
        