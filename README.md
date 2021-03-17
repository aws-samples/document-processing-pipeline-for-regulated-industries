# Document Processing Pipeline For Regulated Industries

## About

This solution implements an ML-integrated document processing pipeline using [Amazon Textract](https://aws.amazon.com/textract), [Amazon Comprehend](https://aws.amazon.com/comprehend), [Amazon Elasticsearch Service](https://aws.amazon.com/elasticsearch-service), and [Amazon S3](https://aws.amazon.com/s3), with a surrounding document governance and lineage service, developed with [Amazon DynamoDB](https://aws.amazon.com/dynamodb), [Amazon SNS](https://aws.amazon.com/sns), [Amazon SQS](https://aws.amazon.com/sqs), and [AWS Lambda](https://aws.amazon.com/lambda/).

## Use Case

Customers in regulated industries want to use machine learning services to automate, annotate, and enhance their static document processing capabilities. However, there are strong compliance and data governance concerns in the field.

This solution is an end to end example of how customers can architect their solutions using asynchronous metadata services, that tie together steps of document processing pipelines, and their outputs, to the original document, thus creating a governance model centered around data lineage, data governance, and documented recordings of uploaded documents.

## Architecture

### Whole Pipeline: Figure 1
![Figure 1](images/pipeline.png)
### Ingestion Module: Figure 2
![Figure 2](images/ingestion.png)
### NLP Module: Figure 3
![Figure 3](images/nlp.png)
### OCR Module: Figure 4
![Figure 4](images/ocr.png)
### Metadata Services Module: Figure 5
![Figure 5](images/metadata-services.png)

## Requirements

* A Linux or MacOS-compatible machine
* An AWS account with sufficient permissions

## Installation

To create the infrastructure, use [aws-cdk](https://github.com/aws/aws-cdk), a software development framework to model and create AWS infrastructure. Use the [npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) package manager to install aws-cdk. Then, download the [AWS CLI](https://aws.amazon.com/cli/).
```
npm install -i aws-cdk@latest
```
Double check you are logged into the AWS account you wish to be deploying the solution to
```
aws configure
aws sts get-caller-identity
```
## Deployment

Navigate to the [code/](code/) folder and execute the build script to
  * setup the python dependencies in Lambda layers
  * copy the code and set up the folder structure for cdk
```
cd code/
bash build.sh
```

Next, navigate to the [infrastructure/](infrastructure/) folder and download the corresponding npm modules used for each AWS resource.
```
cd ../infrastructure 
npm install -i

# To ensure all packages are up to date
npm outdated

# If any packages are outdated, or you think are missing, install/update them using the following command.
npm install -i aws-xxxxx@latest
```
:warning: Installing the latest cdk packages could cause breaking changes :warning:

Once packages are up-to-date, bootstrap the cdk application, and deploy
```
# Bootstrap CDK Application
cdk bootstrap
# Deploy CDK Application
cdk deploy --all
# When prompted for deploying changes, enter 'y' to agree to the resource creation
```
:hourglass_flowing_sand: This action takes a bit of time; there are a lot of resources. If you wish to see the progress, keep track of the [CloudFormation Events](https://console.aws.amazon.com/cloudformation) in the console.

This action should trigger creation of three CloudFormation stacks in the appropriate account, and deploy the resources for the `Metadata`, `Analytics`, and `TextractPipeline` modules.

### Additional Comments
To see a CloudFormation template generated from CDK, use the following command
```
cdk synth --all
```
## Deletion
To trigger stack deletion, execute the destroy command for the application.
```
cdk destroy --all
# When prompted for deploying changes, enter 'y' to agree to the resource creation
```
## Testing

In order to test the pipeline, and witness some results, do the following:

1. In the [Amazon S3 console](https://s3.console.aws.amazon.com/s3/), locate the S3 bucket whose name is `textractpipelinestack-rawdocumentsbucket...`. This is the initial upload bucket that kickstarts the whole process.
1. Upload your favorite PDF to it (one that is preferably not too long).
1. Once the upload succeeds, go ahead and open the [DynamoDB console](https://console.aws.amazon.com/dynamodbv2/) and look at the `MetadataStack-DocumentRegistryTable...`, which is our Registry that defines the documents uploaded and their owners. The document you just uploaded should be referenced here by a unique `documentId` primary key.
1. Next, look at the `MetadataStack-DocumentLineageTable...` that provides the Lineage for the top-level objects, and their paths, created from the document you just uploaded; these are all directly referencing the original document via the `documentId` identifier. Each also has a unique `documentSignature` that identifies that unique object in the S3 space.
1. Further, look at the `MetadataStack-PipelineOperationsTable...` that provides a traceable timeline for each action of the pipeline, as the document is processed through it.
1. In this implementation, the last step of the pipeline is dictated by the `SyncComprehend` NLP Module; that will be reflected in the `Stage` in the Pipeline Operations table. Once that reads `SUCCEEDED`, your document has:
   1. Been analyzed by both Textract and Comprehend
   1. Had both analyses and their outputs poured into each respective S3 bucket (`textractresults`, and `comprehendresults`) following a naming convention.
   1. Had a complete NLP and OCR payload sent to Amazon Elasticsearch.
1. In the `textractresults` S3 bucket, there is a structure put in place for collecting Textract results: 
```s3://<textract results bucket>/<document ID>/<original uploaded file path>/ocr-analysis/page-<number>/<Textract output files in JSON, CSV, and TXT formats>```
If you want to take a look at the original Textract output for the whole document, that file is called `fullresponse.json` found where the page sub-folders are.
1. In the `comprehendresults` S3 bucket, there is also a structure put in place for collecting Comprehend results; this is simply:
```s3://<comprehend results bucket>/<document ID>/<original uploaded file path>/comprehend-output.json```
1. Navigate to the [Elasticsearch console](https://console.aws.amazon.com/es/) and access the Kibana endpoint for that cluster.
1. There should be searchable metadata, and contents of the document you just analyzed, available under the `document` index name in the Kibana user interface. The structure of that JSON metadata should look like this:
```json
{
        'documentId': "asdrfwffg-1234560",  # a unique string UUID generated by the pipeline
        'page'      : 1,  # corresponds to the document page number                           
        'KeyPhrases': [],  # list of key phrases on that page, identified by Comprehend     
        'Entities'  : [], # a list of entities on that page, identified by Comprehend   
        'text'      : "lorem ipsum",  # string raw text on that page, identified by Textract
        'table'     : "Column 1, Column 2...", # string of CSV-formatted tables (if any) on that page, identified by Textract
        'forms'     : ""  # string of forms data (if any) on that page, identified by Textract
}
```

:exclamation::exclamation: By default, only AWS resources in the account can access the Elasticsearch HTTPS endpoint. Make sure to edit the Access Policy to allow for, e.g. your `SourceIP` address. A detailed explanation of controlling Kibana access can be found [here](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-ac.html).

## Extending this Solution

This solution was written to purposely make it easy and straightforward to extend. For example, to add a new pipeline step, all it requires is a Lambda, an invocation trigger, integrating with the metadata services clients (and lineage client if needed), adding some IAM permissions, and naming the pipeline step!

### Data Lake Applications

Data lakes are perfect way to extend this solution; simply plug in the `DocumentRegistry` lambda function to be triggered by any number of S3 buckets, and now the solution will be able to handle any document placed into the lake. The solution does also support versioned buckets for lineage and registration.

Because document registration is **fundamentally decoupled** from document analysis in our ingestion module, you can easily customize which documents in your data lake are sent downstream by configuring the `DocumentClassifier` lambda using business logic to determine, e.g. the class of the document, or given the source bucket, whether this would classify as a reason to analyze or simply just a registration.

### Using other ML products for NLP and OCR

The NLP and OCR tooling itself can be replaced quite easily; the solution provides a pattern for **both** asynchronous and synchronous paths. Some AWS customers might decide to replace Textract or Comprehend with an in-house or 3rd party solution. In that case, the Lambda functions issuing those calls to the OCR or NLP APIs would change, but everything else in the solution should remain very similar.

### Downstream Analytics

The analytics portion of the pipeline just consists of an Elasticsearch cluster for ad-hoc querying. But customers can easily extend the pipeline to add on more data destinations, depending on various use cases. For example, if a customer wants to take the `Table` data produced by Textract, and shuffle it off into a relational database for structured querying and analysis, that could be made possible by creating S3 event triggers for uploads of `tables.csv` objects in the `textractresults` bucket. This method would use the same metadata clients, to deliver events for the lineage and pipeline operations metadata tables, and integrate seamlessly with the rest of the workflow.

### Metadata Services Applications

The metadata services SNS topic provides an asynchronous mechanism for downstream applications or users to receive updates on pipeline progress. For example, if a customer wants to know when their document will be finished, an application user interface can develop an endpoint that subscribes to the SNS topic, looking for `SUCCEEDED` statuses in the message body. If that is received, then the UI can update the document to reflect that.

By the same token, the metadata services can be used to determine unauthorized processing on documents that are not safe to be run through the pipeline for regulatory reasons. The `DocumentClassifier` is precisely the place to flag and halt such cases; if a document is denied, a client message could be sent to the SNS topic determining the reasons, and security teams could take appropriate measures.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

