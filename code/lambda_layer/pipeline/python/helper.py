import boto3
from botocore.client import Config
import os
import csv
import io
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

class DynamoDBHelper:

    @staticmethod
    def deserializeItem(record):
        print(record)
        dynamodb = AwsHelper().getResource('dynamodb')

        deserializer = TypeDeserializer()
        deserializedItem = {k: deserializer.deserialize(v) for k,v in record.items()}
        return deserializedItem
    
    @staticmethod
    def getItems(tableName, key, value):
        items = None

        ddb = AwsHelper().getResource("dynamodb")
        table = ddb.Table(tableName)

        if key is not None and value is not None:
            filter = Key(key).eq(value)
            queryResult = table.query(KeyConditionExpression=filter)
            if(queryResult and "Items" in queryResult):
                items = queryResult["Items"]

        return items

    @staticmethod
    def insertItem(tableName, itemData):

        ddb = AwsHelper().getResource("dynamodb")
        table = ddb.Table(tableName)

        ddbResponse = table.put_item(Item=itemData)

        return ddbResponse

    @staticmethod
    def deleteItems(tableName, key, value, sk):
        items = DynamoDBHelper.getItems(tableName, key, value)
        if(items):
            ddb = AwsHelper().getResource("dynamodb")
            table = ddb.Table(tableName)
            for item in items:
                print("Deleting...")
                print("{} : {}".format(key, item[key]))
                print("{} : {}".format(sk, item[sk]))
                table.delete_item(
                    Key={
                        key: value,
                        sk : item[sk]
                    })
                print("Deleted...")

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

class S3Helper:
    @staticmethod
    def getS3BucketRegion(bucketName):
        client = boto3.client('s3')
        response = client.get_bucket_location(Bucket=bucketName)
        awsRegion = response['LocationConstraint']
        return awsRegion

    @staticmethod
    def writeToS3(content, bucketName, s3FileName, taggingStr=None, awsRegion=None):
        s3 = AwsHelper().getResource('s3', awsRegion)
        object = s3.Object(bucketName, s3FileName)
        if taggingStr:
            object.put(Body=content, Tagging=taggingStr)
        else:
            object.put(Body=content)

    @staticmethod
    def getTagsS3(bucketName, s3FileName, awsRegion=None):
        s3 = AwsHelper().getClient('s3', awsRegion)
        s3_response = s3.get_object_tagging(
            Bucket=bucketName,
            Key=s3FileName
        )
        tag_dict = {}
        for tag in s3_response['TagSet']:
            tag_dict[tag['Key']] = tag['Value']
        return tag_dict
    
    @staticmethod
    def getS3ObjectUrl(bucketName, s3FileName, awsRegion=None):
        s3 = AwsHelper().getClient('s3', awsRegion)
        s3Url = s3.generate_presigned_url(
            'get_object',
            Params = {
                'Bucket': bucketName, 
                'Key': s3FileName
            }
        )
        return s3Url

    @staticmethod
    def tagS3(bucketName, s3FileName, tags=None, awsRegion=None):
        tagset = []
        for key, value in tags.items():
            tag = {
                "Key": key,
                "Value": value
            }
            tagset.append(tag)
            
        s3 = AwsHelper().getClient('s3', awsRegion)
        s3.put_object_tagging(
            Bucket=bucketName,
            Key=s3FileName,
            Tagging={
                'TagSet': tagset
            }
        )

    @staticmethod
    def copyToS3(sourceBucketName, sourceFilename, targetBucketName, targetFileName, awsRegion=None):
        s3 = AwsHelper().getClient('s3', awsRegion)
        copy_source = {
            'Bucket': sourceBucketName,
            'Key': sourceFilename
        }
        s3.copy_object(
            Bucket           = targetBucketName,
            CopySource       = copy_source,
            Key              = targetFileName,
            TaggingDirective = "COPY"
        )
    
    @staticmethod
    def readFromS3(bucketName, s3FileName, awsRegion=None):
        s3 = AwsHelper().getResource('s3', awsRegion)
        obj = s3.Object(bucketName, s3FileName)
        return obj.get()['Body'].read().decode('utf-8')

    @staticmethod
    def writeCSV(fieldNames, csvData, bucketName, s3FileName, awsRegion=None):
        csv_file = io.StringIO()
        #with open(fileName, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldNames)
        writer.writeheader()

        for item in csvData:
            i = 0
            row = {}
            for value in item:
                row[fieldNames[i]] = value
                i = i + 1
            writer.writerow(row)
        S3Helper.writeToS3(csv_file.getvalue(), bucketName, s3FileName)

    @staticmethod
    def writeCSVRaw(csvData, bucketName, s3FileName):
        csv_file = io.StringIO()
        #with open(fileName, 'w') as csv_file:
        writer = csv.writer(csv_file)
        for item in csvData:
            writer.writerow(item)
        S3Helper.writeToS3(csv_file.getvalue(), bucketName, s3FileName)


class FileHelper:
    @staticmethod
    def getFileNameAndExtension(filePath):
        basename = os.path.basename(filePath)
        dn, dext = os.path.splitext(basename)
        return (dn, dext[1:])

    @staticmethod
    def getFileName(fileName):
        basename = os.path.basename(fileName)
        dn, dext = os.path.splitext(basename)
        return dn

    @staticmethod
    def getFileExtension(fileName):
        basename = os.path.basename(fileName)
        dn, dext = os.path.splitext(basename)
        return dext[1:]
