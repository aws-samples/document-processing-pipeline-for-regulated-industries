import sys, os
import json
import boto3
from helper import AwsHelper
import datetime
import pickle

class MetadataClient:
    def __init__(self, targetArn, targetType="sns", region=None, body=None):
        metadataType = "generic"
        
        if region == None:
            region = os.environ.get('AWS_REGION')
        if targetType not in ['sns', 'lambda']:
            raise ValueError("MetadataClient does not accept targets of type {}".format(targetType))
        self._region        = region
        self._targetArn     = targetArn
        self._targetType    = targetType
        self._client        = AwsHelper().getClient(self._targetType, awsRegion=self._region)
        self._metadataType  = metadataType
        self._requiredKeys  = set()
        if body and not isinstance(body, dict):
            raise ValueError("'body' has to be a valid dictionary")
        elif body == None:
            self._body = {}
        else:
            self._body = body

    @property
    def body(self):
        return self._body
    
    @property
    def targetArn(self):
        return self._targetArn
        
    @property
    def targetType(self):
        return self._targetType
    
    @property
    def client(self):
        return self._client
    
    @property
    def requiredKeys(self):
        return self._requiredKeys
    
    @property
    def metadataType(self):
        return self._metadataType
    
    @requiredKeys.setter
    def requiredKeys(self, value):
        self._requiredKeys = value
      
    @body.setter
    def body(self, value):
        self._body = value
    
    def _validate_payload(self, payload):
        if not self.requiredKeys.issubset(set(payload.keys())):
            return False
        return True
    
    def _publishSNS(self, message, messageGroupId, messageAttributes):
        print("publishing to SNS")
        try:
            print(self.client.publish(
                TopicArn       = self.targetArn,    
                Message        = message,
                MessageGroupId = messageGroupId,
                MessageAttributes = {
                    'metadataType': {
                        'DataType'   : 'String',
                        'StringValue': self.metadataType
                    }
                }
            ))
        except Exception as e:
            print(e)
            raise Exception("Unable to publish to topic {}".format(self.targetArn))
    
    
    def publish(self, body, subsetKeys=[]):
        timestamp = str(datetime.datetime.utcnow())
        # Merge existing body with incoming body
        payload = {"timestamp": timestamp, **self.body, **body}

        valid = self._validate_payload(payload)
        if not valid:
            raise ValueError("Incorrect client payload structure. Please double check the required keys!")
            
        documentId  = payload['documentId']
        payloadJson = json.dumps(payload)
        if self.targetType == 'lambda':
            raise ValueError("Not implemented")
        elif self.targetType == 'sns':
            messageAttributes = {
                'metadataType': {
                    'DataType'   : 'String',
                    'StringValue': self.metadataType
                }
            }
            self._publishSNS(payloadJson, documentId, messageAttributes)
        else:
            raise ValueError("Invalid targetType")
    
class PipelineOperationsClient(MetadataClient):
    def __init__(self, targetArn, region=None, targetType="sns", body=None):
        super().__init__(targetArn, targetType, region, body)
        self._metadataType = "pipeline-operations"
        self._requiredKeys = {"documentId", "bucketName", "objectName", "status", "stage"}
    
    def initDoc(self):
        super().publish({
            "status"      : "IN_PROGRESS",
            "initDoc"     : "True" 
        })
    
    def stageInProgress(self, message=None):
        if message:
            super().publish({
                "status"     : "IN_PROGRESS",
                "message"    : message
            })
        else:
            super().publish({
                "status"     : "IN_PROGRESS",
            })
    
    def stageSucceeded(self, message=None):
        if message: 
            super().publish({
                "status"     : "SUCCEEDED",
                "message"    : message
            })
        else:
            super().publish({
                "status"     : "SUCCEEDED"
            })
    
    def stageFailed(self, message=None):
        if message:
            super().publish({
                "status"     : "FAILED",
                "message"    : message
            })
        else:
            super().publish({
                "status"     : "FAILED"
            })
    
class DocumentLineageClient(MetadataClient):
    def __init__(self, targetArn, region=None, targetType="sns", body=None):
        super().__init__(targetArn, targetType, region, body)
        self._metadataType = "document-lineage"
        self._requiredKeys =  {"documentId", "callerId", "targetBucketName", "targetFileName", "s3Event"}

    def recordLineage(self, body):
        print("Recording Lineage")
        print(body)
        if 's3Event' not in body:
            super().publish({"s3Event": "ObjectCreated:Put", **body})
        else:
            super().publish(body)
    
    def recordLineageOfCopy(self, body):
        print("Recording Lineage of S3 Copy")
        print(body)
        super().publish({"s3Event": "ObjectCreated:Copy", **body})
    
class DocumentRegistryClient(MetadataClient):
    def __init__(self, targetArn, region=None, targetType="sns", body=None):
        super().__init__(targetArn, targetType, region, body)
        self._metadataType = "document-registry"
        self._requiredKeys =  {"documentId", "bucketName", "documentName", "documentLink", "principalIAMWriter"}

    def registerDocument(self, body):
        print(body)
        super().publish(body)