import json
import os
import boto3
import time
from helper import AwsHelper, S3Helper
from og import OutputGenerator
from metadata import PipelineOperationsClient, DocumentLineageClient

PIPELINE_STAGE = "ASYNC_PROCESS_TEXTRACT"

textractBucketName = os.environ.get("TARGET_TEXTRACT_BUCKET_NAME", None)
metadataTopic  = os.environ.get('METADATA_SNS_TOPIC_ARN', None)

if not textractBucketName or not metadataTopic:
    raise ValueError("Missing arguments.")

pipeline_client = PipelineOperationsClient(metadataTopic)
lineage_client = DocumentLineageClient(metadataTopic)

def getJobResults(api, jobId, objectName):

    resultJSON = []

    s3_helper = S3Helper()
    textractRawResultsFiles = s3_helper.listObjectsInS3(
        bucketName   = textractBucketName,
        bucketPrefix = objectName + "/textract-output/" + jobId
    )
    # skip the s3 access file, which will always appear first
    for textractResultFile in textractRawResultsFiles[1:]:
        resultJSON.append(json.loads(s3_helper.readFromS3(textractBucketName, textractResultFile)))
    
    # time.sleep(5)

    # client = AwsHelper().getClient('textract')
    # if(api == "StartDocumentTextDetection"):
    #     response = client.get_document_text_detection(JobId=jobId)
    # else:
    #     response = client.get_document_analysis(JobId=jobId)
    # pages.append(response)
    # print("Resultset page received: {}".format(len(pages)))
    # nextToken = None
    # if('NextToken' in response):
    #     nextToken = response['NextToken']
    #     print("Next token: {}".format(nextToken))

    # while(nextToken):
    #     time.sleep(5)

    #     if(api == "StartDocumentTextDetection"):
    #         response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
    #     else:
    #         response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

    #     pages.append(response)
    #     print("Resultset page received: {}".format(len(pages)))
    #     nextToken = None
    #     if('NextToken' in response):
    #         nextToken = response['NextToken']
    #         print("Next token: {}".format(nextToken))

    return resultJSON

def processRequest(request):

    output = ""
    status = request['jobStatus']
    jobId = request['jobId']
    jobTag = request['jobTag']
    jobAPI = request['jobAPI']
    bucketName = request['bucketName']
    objectName = request['objectName']
    
    pipeline_client.body = {
        "documentId": jobTag,
        "bucketName": bucketName,
        "objectName": objectName,
        "stage":      PIPELINE_STAGE
    }
    if status == 'FAILED':
        pipeline_client.stageFailed("Textract job for document ID {}; bucketName {} fileName {}; failed during Textract analysis. Please double check the document quality".format(jobTag, bucketName, objectName))
        raise Exception("Textract Analysis didn't complete successfully")
    
    pipeline_client.stageInProgress()
    try:
       resultJSON = getJobResults(jobAPI, jobId, objectName)
    except Exception as e:
        pipeline_client.stageFailed("Textract job for document ID {}; bucketName {} filename {} failed during Textract processing. Could not read Textract output files under job Name {}".format(jobTag, bucketName, objectName, jobId))
        raise Exception("Textract Analysis didn't complete successfully")
        
    print("Result Textract result objects received: {}".format(len(resultJSON)))

    detectForms = False
    detectTables = False
    if(jobAPI == "StartDocumentAnalysis"):
        detectForms = True
        detectTables = True

    try:
        opg = OutputGenerator(
            documentId = jobTag,
            response   = resultJSON,
            bucketName = textractBucketName,
            objectName = objectName,
            forms      = detectForms,
            tables     = detectTables
        )
    except Exception as e:
        pipeline_client.stageFailed("Could not convert results from Textract into processable object. Try uploading again.")
        raise(e)
        
    tagging = "documentId={}".format(jobTag)
    opg.writeTextractOutputs(taggingStr=tagging)
    
    lineage_client.recordLineage({
        "documentId":       jobTag,
        "callerId":         request["callerId"],
        "sourceBucketName": bucketName,
        "targetBucketName": textractBucketName,
        "sourceFileName":   objectName,
        "targetFileName":   objectName
    })
    
    output = "Processed -> Document: {}, Object: {}/{} processed.".format(jobTag, bucketName, objectName)
    pipeline_client.stageSucceeded()
    print(output)
    return {
        'statusCode': 200,
        'body': output
    }

def lambda_handler(event, context):

    print("event: {}".format(event))

    body = json.loads(event['Records'][0]['body'])
    message = json.loads(body['Message'])

    print("Message: {}".format(message))

    request = {}

    request["jobId"]        = message['JobId']
    request["jobTag"]       = message['JobTag']
    request["jobStatus"]    = message['Status']
    request["jobAPI"]       = message['API']
    request["bucketName"]   = message['DocumentLocation']['S3Bucket']
    request["objectName"]   = message['DocumentLocation']['S3ObjectName']
    request["callerId"]     = context.invoked_function_arn
    return processRequest(request)
