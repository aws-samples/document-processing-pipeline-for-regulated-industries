import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from helper import AwsHelper
from datetime import datetime

class DocumentRegistryStore:
    def __init__(self, documentRegistryName):
        self._registryTableName = documentRegistryName
    
    def registerDocument(self, documentId, bucketName, documentName, documentLink, principalIAMWriter, timestamp, documentMetadata, documentVersion=None):
        ret = None
        
        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._registryTableName)
        item = {
            "documentId": documentId,
            "principalIAMWriter": principalIAMWriter,
            "bucketName": bucketName,
            "documentName": documentName,
            "documentLink": documentLink,
            "documentMetadata": documentMetadata,
            "timestamp": timestamp
        }
        if documentVersion:
            item['documentVersion'] = documentVersion
        try:
            table.put_item(
                ConditionExpression = "attribute_not_exists(documentId)",
                Item = item
            )
            ret = {
                'Status': 200
            }
        except ClientError as e:
            print(e)
            ret = {
                'Error': e.response['Error']['Message'],
                'Status': e.response['ResponseMetadata']['HTTPStatusCode']
            }
        except Exception as e:
            print(e)
            ret = {
                'Error': 'Unknown error occurred during updating document',
                'Status': 400
            }
        return ret
    
class LineageStore:
    def __init__(self, lineageTableName, lineageIndexName):
        self._lineageTableName = lineageTableName
        self._lineageIndexName = lineageIndexName
    
    def createLineage(self, documentId, callerId, targetBucketName, targetFileName, timestamp, s3Event, sourceBucketName=None, sourceFileName=None, versionId=None):
        ret = None
        
        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._lineageTableName)
        documentSignature = "BUCKET:{}@FILE:{}".format(targetBucketName, targetFileName)
        if versionId:
            documentSignature += "@VERSION:{}".format(versionId)
        item = {
            "documentId": documentId,
            "documentSignature": documentSignature,
            "callerId": callerId,
            "targetBucketName": targetBucketName,
            "targetFileName": targetFileName,
            "timestamp": timestamp,
            "s3Event": s3Event
        }
        if versionId:
            item['versionId'] = versionId
        if sourceFileName:
            item['sourceFileName'] = sourceFileName
        if sourceBucketName:
            item['sourceBucketName'] = sourceBucketName
        try:
            table.put_item(
                Item = item
            )
            ret = {
                'Status': 200
            }
        except ClientError as e:
            print(e)
            ret = {
                'Error': e.response['Error']['Message'],
                'Status': e.response['ResponseMetadata']['HTTPStatusCode']
            }
        except Exception as e:
            print(e)
            ret = {
                'Error': 'Unknown error occurred during updating document',
                'Status': 400
            }
        return ret
        
    def queryDocumentId(self, targetBucketName, targetFileName, versionId=None):
        ret = None
        res = None
        
        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._lineageTableName)
        documentSignature = "BUCKET:{}@FILE:{}".format(targetBucketName, targetFileName)
        if versionId:
            documentSignature += "@VERSION:{}".format(versionId)
        try:
            res = table.query(
                KeyConditionExpression = Key('documentSignature').eq(documentSignature),
                IndexName = self._lineageIndexName
            )
        except ClientError as e:
            print(e)
            ret = {
                'Error': e.response['Error']['Message'],
                'Status': e.response['ResponseMetadata']['HTTPStatusCode']
            }
        except Exception as e:
            print(e)
            ret = {
                'Error': 'Unknown error occurred during querying the document Id',
                'Status': 400
            }
        try:
            items = res['Items']
            print(items)
            if len(items) == 0:
                ret = {
                    'Status': 404,
                    'documentId': None
                }
            else:
                items.sort(key=lambda item: datetime.fromisoformat(item['timestamp']))
                ret = {
                    'Status': 200,
                    'documentId': items[0]['documentId']
                }
        except Exception as e:
            print(e)
            ret = {
                'Error': 'Could not find the documentId for specified document Signature',
                'Status': 400
            }
            
        return ret

class PipelineOpsStore:

    def __init__(self, opsTableName):
        self._opsTableName = opsTableName

    def startDocumentTracking(self, documentId, bucketName, objectName, status, stage, timestamp, versionId=None):

        ret = None
        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._opsTableName)
        item = {
            "documentId": documentId,
            "bucketName": bucketName,
            "objectName": objectName,
            "documentStatus": status,
            "documentStage": stage,
            "lastUpdate": timestamp,
            "timeline": [{
                "timestamp": timestamp,
                "stage": stage,
                "status": status
            }]
        }
        if versionId:
            item['documentVersion'] = versionId
        try:
            table.put_item(
                ConditionExpression = "attribute_not_exists(documentId)",
                Item = item
            )
            ret = {
                'Status': 200
            }
        except ClientError as e:
            print(e)
            ret  = {
                'Status': e.response['ResponseMetadata']['HTTPStatusCode'],
                'Error': e.response['Error']['Message']
            }
        except Exception as e:
            print(e)
            ret = {
                'Error': 'Unknown error occurred during updating document',
                'Status': 400
            }
        return ret

    def updateDocumentStatus(self, documentId, status, stage, timestamp, message=None):

        ret = None

        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._opsTableName)
        try:
            if message:
                new_datapoint = {
                    "timestamp": timestamp,
                    "stage": stage,
                    "status": status,
                    "message": message
                }
            else:
                new_datapoint = {
                    "timestamp": timestamp,
                    "stage": stage,
                    "status": status
                }
            table.update_item(
                Key = {
                    'documentId': documentId
                },
                UpdateExpression = 'SET documentStatus = :documentStatus, documentStage = :documentStage, lastUpdate = :lastUpdate, timeline = list_append(timeline, :new_datapoint)',
                ConditionExpression = 'attribute_exists(documentId)',
                ExpressionAttributeValues = {
                    ':documentStatus': status,
                    ':documentStage': stage,
                    ':lastUpdate': timestamp,
                    ':new_datapoint': [new_datapoint]
                }
            )
            ret = {
                'Status': 200
            }
        except ClientError as e:
            print(e)
            ret  = {
                'Error' : e.response['Error']['Message'],
                'Status': e.response['ResponseMetadata']['HTTPStatusCode']
            }
        except Exception as e:
            print(e)
            ret = {
                'Error' : 'Updating document failed',
                'Status': 400
            }

        return ret

    def markDocumentComplete(self, documentId, stage, timestamp):

        return self.updateDocumentStatus(documentId, "SUCCEEDED", stage, timestamp)

    def getDocument(self, documentId):

        dynamodb = AwsHelper().getClient("dynamodb")

        ddbGetItemResponse = dynamodb.get_item(
            Key={'documentId': {'S': documentId} },
            TableName=self._opsTableName
        )

        itemToReturn = None

        if('Item' in ddbGetItemResponse):
            itemToReturn = { 'documentId' : ddbGetItemResponse['Item']['documentId']['S'],
                             'bucketName' : ddbGetItemResponse['Item']['bucketName']['S'],
                             'objectName' : ddbGetItemResponse['Item']['objectName']['S'],
                             'status'     : ddbGetItemResponse['Item']['documentStatus']['S'],
                             'stage'      : ddbGetItemResponse['Item']['documentStage']['S']}

        return itemToReturn

    def deleteDocument(self, documentId):

        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._opsTableName)

        table.delete_item(
            Key={
                'documentId': documentId
            }
        )

    def getDocuments(self, nextToken=None):

        dynamodb = AwsHelper().getResource("dynamodb")
        table = dynamodb.Table(self._opsTableName)

        pageSize = 25

        if(nextToken):
            response = table.scan(ExclusiveStartKey={ "documentId" : nextToken}, Limit=pageSize)
        else:
            response = table.scan(Limit=pageSize)

        print("response: {}".format(response))

        data = []

        if('Items' in response):        
            data = response['Items']

        documents = { 
            "documents" : data
        }

        if 'LastEvaluatedKey' in response:
            nextToken = response['LastEvaluatedKey']['documentId']
            print("nexToken: {}".format(nextToken))
            documents["nextToken"] = nextToken

        return documents