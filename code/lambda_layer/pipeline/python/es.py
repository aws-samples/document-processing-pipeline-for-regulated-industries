import os
import json
import urllib.parse
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import requests
from pprint import pprint
from requests_aws4auth import AWS4Auth

# Set by lambda itself
default_region = os.environ['AWS_REGION']

class ESCluster:
    def __init__(self, host, use_ssl=True, port=443, verify_certs=True, region=None):
        if default_region:
            self._region = default_region
        else:
            self._region = region
        self._host = host
        self._port = port
        self._verify_certs = verify_certs
        self._use_ssl = use_ssl
        
    def connect(self):
        print ("Connecting to the ES Endpoint {}".format(self._host))
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key, 
            credentials.secret_key, 
            self._region, 'es',
            session_token=credentials.token)
        try:
            self._connection = Elasticsearch(
                    hosts=[
                        {'host': self._host, 'port': self._port}
                    ],
                    http_auth=awsauth,
                    use_ssl=self._use_ssl,
                    verify_certs=self._verify_certs,
                    connection_class=RequestsHttpConnection)
            print("Successfully established connection to {}".format(self._host))       
            return self._connection
        except Exception as E:
            print("Unable to connect to {}".format(self._host))
            print(E)
    
    def post(self, index, payload, doctype="_doc"):
        self._connection.index(index=index, doc_type=doctype, body=payload)
    
    def post_bulk(self, index, payload, doctype="_doc"):
        if isinstance(payload, list):
            helpers.bulk(self._connection, payload, index=index, doc_type=doctype)
        else:
            raise Exception("Non-iterable payload detected")