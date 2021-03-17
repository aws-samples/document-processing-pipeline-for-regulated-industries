import boto3
from botocore.client import Config
import os
import csv
import io
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

class SQSHelper:
    
    @staticmethod
    def deleteMessage(queueArn, receipt):
        sqs = AwsHelper().getClient("sqs")
    
        queueParts = queueArn.split(":")
        queueName = queueParts[-1]
        queueAccount = queueParts[-2]
        print(queueName)
        print(queueAccount)
        try:
            queueUrl = sqs.get_queue_url(
                QueueName              = queueName,
                QueueOwnerAWSAccountId = queueAccount
            )['QueueUrl']
            
            sqs.delete_message(
                QueueUrl      = queueUrl,
                ReceiptHandle = receipt
            )
        except Exception as e:
            raise(e)

class AwsHelper:
    def getClient(self, name, awsRegion=None):
        config = Config(
            retries = dict(
                max_attempts = 30
            )
        )
        if(awsRegion):
            return boto3.client(name, region_name=awsRegion, config=config)
        else:
            return boto3.client(name, config=config)

    def getResource(self, name, awsRegion=None):
        config = Config(
            retries = dict(
                max_attempts = 30
            )
        )

        if(awsRegion):
            return boto3.resource(name, region_name=awsRegion, config=config)
        else:
            return boto3.resource(name, config=config)
