import os, sys
import json
import urllib.parse
import boto3
from trp import Document
from helper import S3Helper, AwsHelper, FileHelper
from es import ESCluster
import requests
from pprint import pprint
from og import OutputGenerator
from aws_requests_auth.aws_auth import AWSRequestsAuth
from requests_aws4auth import AWS4Auth
from metadata import PipelineOperationsClient, DocumentLineageClient

PIPELINE_STAGE = "SYNC_PROCESS_COMPREHEND"

COMPREHEND_CHARACTER_LIMIT = 4096

metadataTopic      = os.environ.get('METADATA_SNS_TOPIC_ARN', None)
comprehendBucket   = os.environ.get('TARGET_COMPREHEND_BUCKET', None)
esCluster          = os.environ.get('TARGET_ES_CLUSTER', None)
esIndex            = os.environ.get('ES_CLUSTER_INDEX', "document")

if not esCluster or not comprehendBucket or not metadataTopic:
    raise Exception("Missing arguments.")

pipeline_client = PipelineOperationsClient(metadataTopic)
lineage_client  = DocumentLineageClient(metadataTopic)
es              = ESCluster(host=esCluster)

def dissectObjectName(objectName):
    objectParts = objectName.split("/ocr-analysis/")
    objectPath = objectParts[0].split("/")
    documentId  = objectPath[0]
    documentName = "/".join(objectPath[1:])
    return (documentId, documentName)
    
def chunkUpTheText(text):
    chunksOfText = []
    while(len(text) > COMPREHEND_CHARACTER_LIMIT):
        indexSnip = COMPREHEND_CHARACTER_LIMIT
        while(not text[indexSnip].isspace()):
            indexSnip += 1
        chunksOfText.append(text[0:indexSnip])
        text = text[indexSnip:]
    chunksOfText.append(text)
    return chunksOfText
    
def batchSendToComprehend(comprehend, textList, language):
    keyPhrases = set()
    entitiesDetected = {}
    try:
        keyphrase_response = comprehend.batch_detect_key_phrases(TextList=textList, LanguageCode=language)
        keyphraseResultsList = keyphrase_response.get("ResultList")
        for keyphraseListResp in keyphraseResultsList:
            keyphraseList = keyphraseListResp.get('KeyPhrases')
            for s in keyphraseList:
                s_txt = s.get("Text").strip('\t\n\r')
                print("Detected keyphrase {}".format(s_txt))
                keyPhrases.add(s_txt)
    except Exception as e:
        pipeline_client.stageFailed("Could not batch detect key phrases in Comprehend")
        raise(e)
    try:
        detect_entity_response = comprehend.batch_detect_entities(TextList=textList, LanguageCode=language)
        entityResultsList = detect_entity_response.get("ResultList")
        for entitiesListResp in entityResultsList:
            entityList = entitiesListResp.get("Entities")
            for s in entityList:
                entitiesDetected.update([(s.get("Type").strip('\t\n\r'), s.get("Text").strip('\t\n\r'))])
    except Exception as e:
        pipeline_client.stageFailed("Could not batch detect entities in batch in Comprehend")
        raise(e)
    
    return (list(keyPhrases), entitiesDetected)

def singularSendToComprehend(comprehend, text, language):
    keyPhrases = set()
    entitiesDetected = {}
    try:
        keyphrase_response = comprehend.detect_key_phrases(Text=text, LanguageCode=language)
        keyphraseList = keyphrase_response.get("KeyPhrases")
        for s in keyphraseList:
            s_txt = s.get("Text").strip('\t\n\r')
            print("Detected keyphrase {}".format(s_txt))
            keyPhrases.add(s_txt)
    except Exception as e:
        pipeline_client.stageFailed("Could not detect key phrases in Comprehend")
        raise(e)
    try:
        detect_entity = comprehend.detect_entities(Text=text, LanguageCode=language)
        entityList = detect_entity.get("Entities")
        for s in entityList:
            entitiesDetected.update([(s.get("Type").strip('\t\n\r'),s.get("Text").strip('\t\n\r'))])
    except Exception as e:
        pipeline_client.stageFailed("Could not detect entities in Comprehend")
        raise(e)
    
    return (list(keyPhrases), entitiesDetected)
    
def runComprehend(bucketName, objectName, callerId):
    
    comprehend = AwsHelper().getClient('comprehend')
    documentId, documentName = dissectObjectName(objectName)
    assert (documentId == S3Helper().getTagsS3(bucketName, objectName).get('documentId', None)), "File path {} does not match the expected documentId tag of the object triggered.".format(objectName)
    
    textractOutputJson = json.loads(S3Helper().readFromS3(bucketName, objectName))
    og = OutputGenerator(response=textractOutputJson, forms=False, tables=False)
    
    pipeline_client.body = {
        "documentId": documentId,
        "bucketName": bucketName,
        "objectName": objectName,
        "stage":      PIPELINE_STAGE
    }
    pipeline_client.stageInProgress()    
    
    document = Document(textractOutputJson)
    originalFileName = "{}/{}".format(documentId, documentName)
    comprehendFileName = originalFileName + "/comprehend-output.json"
    comprehendFileS3Url = "https://{}.s3.amazonaws.com/{}".format(comprehendBucket, urllib.parse.quote_plus(comprehendFileName, safe="/"))
    tagging = "documentId={}".format(documentId)
    
    es.connect()
    esPayload = []
    page_num = 1
    for page in document.pages:
        table = og.structurePageTable(page)
        forms = og.structurePageForm(page)
        text = og.structurePageText(page)

        keyPhrases = []
        entitiesDetected = {}
        
        lenOfEncodedText = len(text)
        print("Comprehend documentId {} processing page {}".format(documentId, str(page_num)))
        print("Length of encoded text is " + str(lenOfEncodedText))
        if lenOfEncodedText > COMPREHEND_CHARACTER_LIMIT:
            print("Size was too big to run singularly; breaking up the page text into chunks")
            try:
                chunksOfText = chunkUpTheText(text)
            except Exception as e:
                pipeline_client.stageFailed("Could not determine how to snip the text on page {} into chunks.".format(page_num))
                raise(e)
            keyPhrases, entitiesDetected = batchSendToComprehend(comprehend, chunksOfText, 'en')
        else:
            keyPhrases, entitiesDetected = singularSendToComprehend(comprehend, text, 'en')
            
        esPageLoad = compileESPayload(es, page_num, keyPhrases, entitiesDetected, text, table, forms, documentId)
        esPayload.append(esPageLoad)
        page_num = page_num + 1
    
    try:
        es.post_bulk(index=esIndex, payload=esPayload)
    except Exception as e:
        pipeline_client.stageFailed("Could not post to Elasticsearch")
        raise(e)
    
    print("Data uploaded to ES")
    try:
        S3Helper().writeToS3(json.dumps(esPayload), comprehendBucket, comprehendFileName, taggingStr=tagging)
    except Exception as e:
        pipeline_client.stageFailed("Failed to write comprehend payload to S3")
        raise(e)
        
    lineage_client.recordLineage({
        "documentId":       documentId,
        "callerId":         callerId,
        "sourceBucketName": bucketName,
        "targetBucketName": comprehendBucket,
        "sourceFileName":   objectName,
        "targetFileName":   comprehendFileName
    })
    pipeline_client.stageSucceeded()
    print("Comprehend data uploaded to S3 at {}".format(comprehendFileName))
    
def compileESPayload(esCluster, pageNum, keyPhrases, entitiesDetected, text, table, forms, documentId):
    payload = {
        'documentId': documentId,
        'page'      : pageNum,
        'KeyPhrases': keyPhrases,
        'Entities'  : entitiesDetected,
        'text'      : text,
        'table'     : table,
        'forms'     : forms
    }
    pprint(payload)
    return payload

def lambda_handler(event, context):
    print("Comprehend Event: {}".format(event))

    bucketName = event['Records'][0]['s3']['bucket']['name']
    objectName = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    callerId   = context.invoked_function_arn
    assert (FileHelper().getFileNameAndExtension(objectName.lower()) == ('fullresponse', 'json')), "File detected does not match expected format: 'fullresponse.json'"
    
    runComprehend(bucketName, objectName, callerId)
