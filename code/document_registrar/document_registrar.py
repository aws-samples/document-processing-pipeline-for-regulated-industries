import json
import os
import uuid
import urllib.parse
from metadata import DocumentRegistryClient, DocumentLineageClient
from helper import FileHelper, S3Helper

metadataTopic  = os.environ.get('METADATA_SNS_TOPIC_ARN', None)

if not metadataTopic:
    raise ValueError("Missing arguments.")

## The body should be customized with the document Metadata to then allow for the objects to be relayed (or not)
## Body could be assessed from existing S3 object metadata
## to the NLP pipeline.
## The logic for assessing the metadata is in the document_classifier function

registry_client = DocumentRegistryClient(
    metadataTopic,
    body = {
        "documentMetadata": {
            "owner": "CustomerName",
            "class": "external_public_report"
        }
    }
)
lineage_client = DocumentLineageClient(metadataTopic)

def processCreateRequest(bucketName, documentName, documentVersion, principalIAMWriter, eventName):
    documentId = str(uuid.uuid1())
    output = ""
  
    print("Input Object: {}/{} version {}".format(bucketName, documentName, documentVersion))
    print("Tagging object {} with tag {} and version {}".format(documentName, documentId, documentVersion))
    S3Helper().tagS3(bucketName, documentName, tags={"documentId": documentId})
    try:
        documentLink = "s3://" + bucketName + "/" + urllib.parse.quote_plus(documentName)
        registryItem = {
            "documentId":   documentId,
            "bucketName":   bucketName,
            "documentName": documentName,
            "documentLink": documentLink,
            "principalIAMWriter": principalIAMWriter
        }
        lineageItem = {
            "documentId": documentId,
            "callerId": principalIAMWriter,
            "targetBucketName": bucketName,
            "targetFileName": documentName,
            "s3Event": eventName
        }
        if documentVersion:
            registryItem['documentVersion'] = documentVersion
            lineageItem['versionId'] = documentVersion
        registry_client.registerDocument(registryItem)
        lineage_client.recordLineage(lineageItem)
        output = "Saved document {} for {}/{} version {}".format(documentId, bucketName, documentName, documentVersion)
    except Exception as e:
        print(e)
        raise(e)
    print(output)
    
def processDeleteRequest(bucketName, documentName, documentVersion, principalIAMWriter, eventName):
    print("Remove Object Processing: {}/{} version {}".format(bucketName, documentName, documentVersion))
    try:
        documentLink = "s3://" + bucketName + "/" + urllib.parse.quote_plus(documentName)
        lineageItem = {
            "documentId": "UNKNOWN_YET",
            "callerId": principalIAMWriter,
            "targetBucketName": bucketName,
            "targetFileName": documentName,
            "s3Event": eventName
        }
        if documentVersion:
            lineageItem['versionId'] = documentVersion
        lineage_client.recordLineage(lineageItem)
        output = "Marked document {}/{} with version {} for deletion".format(bucketName, documentName, documentVersion)
    except Exception as e:
        print(e)
        raise(e)
    print(output)

def lambda_handler(event, context):

    print("event: {}".format(event))
    for record in event['Records']:
        if 'eventSource' in record and record['eventSource'] == 'aws:s3':
            bucketName = record['s3']['bucket']['name']
            documentName = urllib.parse.unquote_plus(record['s3']['object']['key'])
            documentVersion = record['s3']['object'].get('versionId', None)
            principalIAMWriter = record['userIdentity']['principalId']
            eventName = record['eventName']
            if eventName == "ObjectRemoved:Delete":
                processDeleteRequest(bucketName, documentName, documentVersion, principalIAMWriter, eventName)
            elif eventName.startswith("ObjectCreated"):
                processCreateRequest(bucketName, documentName, documentVersion, principalIAMWriter, eventName)
            else:
                print("Processing not yet implemented")
        else:
            print("Uninvoked recorded event structure.")