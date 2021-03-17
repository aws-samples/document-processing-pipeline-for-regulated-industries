import os
import boto3, json
from datastore import PipelineOpsStore
from helper import AwsHelper, SQSHelper 

PIPELINE_OPS_TABLE = os.environ.get("PIPELINE_OPS_TABLE", None)
SQS_QUEUE_ARN   = os.environ.get("SQS_QUEUE_ARN", None)

if not PIPELINE_OPS_TABLE or not SQS_QUEUE_ARN:
    raise ValueError("Missing arguments.")
    
def startDocumentTracking(documentPayload, receipt):
    print("Started tracking document {}".format(documentPayload['documentId']))
    client = PipelineOpsStore(PIPELINE_OPS_TABLE)
    
    res = client.startDocumentTracking(**documentPayload)
    print(res)
    if res['Status'] == 200:
        SQSHelper().deleteMessage(SQS_QUEUE_ARN, receipt)
    else:
        raise Exception("Unable to post document {}: {}".format(documentPayload['documentId'], res['Error']))
    return res

def updateDocumentStatus(documentPayload, receipt, messageNote=None):
    print("Putting pipeline document status update")
    client = PipelineOpsStore(PIPELINE_OPS_TABLE)
    if messageNote:
        statusPayload = {
            "documentId": documentPayload['documentId'],
            "status":     documentPayload['status'],
            "stage":      documentPayload['stage'],
            "timestamp":  documentPayload['timestamp'],
            "message":    messageNote
        }
    else:
        statusPayload = {
            "documentId": documentPayload['documentId'],
            "status":     documentPayload['status'],
            "stage":      documentPayload['stage'],
            "timestamp":  documentPayload['timestamp']
        }
    res = client.updateDocumentStatus(**statusPayload)
    print(res)
    if res['Status'] == 200:
        SQSHelper().deleteMessage(SQS_QUEUE_ARN, receipt)
    else:
        raise Exception("Unable to update status of document {}: {}".format(statusPayload['documentId'], res['Error']))
    return res

def lambda_handler(event, context):
    for record in event['Records']:
        print(event)
        assert record['eventSourceARN'] == SQS_QUEUE_ARN, "Unexpected Lambda event source ARN. Expected {}, got {}".format(SQS_QUEUE_ARN, record['eventSourceARN'])
        payload = json.loads(record["body"])
        message = json.loads(payload['Message'])
        receipt = record['receiptHandle']
        print(message)
        try:
            documentPayload = {
                "documentId": message['documentId'],
                "bucketName": message['bucketName'],
                "objectName": message['objectName'],
                "status":     message['status'],
                "stage":      message['stage'],
                "timestamp":  message['timestamp'],
            }
        except Exception as e:
            print("Missing " + str(e))
            raise ValueError("Missing parameters in payload to pipeline metadata lambda")
        if 'initDoc' in message and message.get('initDoc') == "True":
            startDocumentTracking(documentPayload, receipt)
        else:
            messageNote = message.get('message')
            updateDocumentStatus(documentPayload, receipt, messageNote)
            