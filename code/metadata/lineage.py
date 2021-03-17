import os
import boto3, json
from datastore import LineageStore
from helper import AwsHelper, SQSHelper

DOCUMENT_LINEAGE_TABLE = os.environ.get("DOCUMENT_LINEAGE_TABLE", None)
DOCUMENT_LINEAGE_INDEX = os.environ.get("DOCUMENT_LINEAGE_INDEX", None)
SQS_QUEUE_ARN = os.environ.get("SQS_QUEUE_ARN", None)

if not DOCUMENT_LINEAGE_TABLE or not DOCUMENT_LINEAGE_INDEX or not SQS_QUEUE_ARN:
    raise ValueError("Missing arguments.")

def postLineage(lineagePayload, receipt):
    client = LineageStore(DOCUMENT_LINEAGE_TABLE, DOCUMENT_LINEAGE_INDEX)
    
    if lineagePayload['s3Event'].startswith("ObjectRemoved"):
        res = client.queryDocumentId(
            lineagePayload['targetBucketName'],
            lineagePayload['targetFileName'],
            lineagePayload.get('versionId')
        )
        if res['Status'] == 200:
            actualDocumentId = res['documentId']
            lineagePayload['documentId'] = actualDocumentId
        elif res['Status'] == 404:
            print("Could not find corresponding documentId for this deletion event")
            res = SQSHelper().deleteMessage(SQS_QUEUE_ARN, receipt)
            return res
        else:
            raise Exception("Unable to update deletion of document {}/{} Version {}: {}".format(
                lineagePayload['targetBucketName'], lineagePayload['targetFileName'], lineagePayload.get('versionId'), res['Error']))
            
    res = client.createLineage(**lineagePayload)
    if res['Status'] == 200:
        SQSHelper().deleteMessage(SQS_QUEUE_ARN, receipt)
    else:
        raise Exception("Unable to update progress of document {}: {}".format(lineagePayload['documentId'], res['Error']))
    return res

def lambda_handler(event, context):
    for record in event['Records']:
        print(event)
        assert record['eventSourceARN'] == SQS_QUEUE_ARN, "Unexpected Lambda event source ARN. Expected {}, got {}".format(SQS_QUEUE_ARN, record['eventSourceARN'])
        payload = json.loads(record["body"])
        message = json.loads(payload['Message'])
        print(message)
        receipt = record['receiptHandle']
        lineagePayload = {}
        try:
            lineagePayload = {
                "documentId":       message['documentId'],
                "callerId":         message['callerId'],
                "targetFileName":   message['targetFileName'],
                "targetBucketName": message['targetBucketName'],
                "timestamp":        message['timestamp'],
                "s3Event":          message['s3Event']
            }
            if 'versionId' in message:
                lineagePayload['versionId'] = message['versionId']
        except Exception as e:
            print(e)
            raise ValueError("Missing parameters in payload to lineage lambda")
        try:
            if message['s3Event'] == 'ObjectCreated:Copy':
                lineagePayload['sourceBucketName'] = message['sourceBucketName']
                lineagePayload['sourceFileName']   = message['sourceFileName']
        except:
            print(e)
            raise ValueError("Missing parameters from Copy Object S3 notification")
        postLineage(lineagePayload, receipt)
        