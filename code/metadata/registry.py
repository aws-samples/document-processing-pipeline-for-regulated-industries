import os
import boto3, json
from datastore import DocumentRegistryStore
from helper import AwsHelper, SQSHelper

REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", None)
SQS_QUEUE_ARN = os.environ.get("SQS_QUEUE_ARN", None)

if not REGISTRY_TABLE or not SQS_QUEUE_ARN:
    raise ValueError("Missing arguments.")

def postRegistration(documentInfoPayload, receipt):
    print("posting Registration")
    client = DocumentRegistryStore(REGISTRY_TABLE)
    
    res = client.registerDocument(**documentInfoPayload)
    if res['Status'] == 200:
        SQSHelper().deleteMessage(SQS_QUEUE_ARN, receipt)
    else:
        raise Exception("Unable to update progress of document {}: {}".format(documentInfoPayload['documentId'], res['Error']))
    return res

def lambda_handler(event, context):
    for record in event['Records']:
        print(event)
        assert record['eventSourceARN'] == SQS_QUEUE_ARN, "Unexpected Lambda event source ARN. Expected {}, got {}".format(SQS_QUEUE_ARN, record['eventSourceARN'])
        payload = json.loads(record["body"])
        message = json.loads(payload['Message'])
        print(message)
        receipt = record['receiptHandle']
        registryPayload = {}
        try:
            registryPayload = {
                "documentId":         message['documentId'],
                "bucketName":         message['bucketName'],
                "documentName":       message['documentName'],
                "documentMetadata":   message.get('documentMetadata', dict()),
                "documentLink":       message['documentLink'],
                "principalIAMWriter": message['principalIAMWriter'],
                "timestamp":          message['timestamp'],
            }
            if 'documentVersion' in message:
                registryPayload['documentVersion'] = message['documentVersion']
        except Exception as e:
            print(e)
            raise ValueError("Missing parameters in payload to document registry lambda")
            
        postRegistration(registryPayload, receipt)
        