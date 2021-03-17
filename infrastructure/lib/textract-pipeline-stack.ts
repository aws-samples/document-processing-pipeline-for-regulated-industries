import * as cdk from '@aws-cdk/core';
import iam = require('@aws-cdk/aws-iam');
import { S3EventSource, SqsEventSource, SnsEventSource, DynamoEventSource } from '@aws-cdk/aws-lambda-event-sources';
import sns = require('@aws-cdk/aws-sns');
import snsSubscriptions = require("@aws-cdk/aws-sns-subscriptions");
import sqs = require('@aws-cdk/aws-sqs');
import dynamodb = require('@aws-cdk/aws-dynamodb');
import lambda = require('@aws-cdk/aws-lambda');
import s3 = require('@aws-cdk/aws-s3');
import es = require('@aws-cdk/aws-elasticsearch');

interface MultistackProps extends cdk.StackProps {
  pipelineOpsTable : dynamodb.Table;
  lineageTable : dynamodb.Table;
  indexName : string;
  documentRegistryTable : dynamodb.Table;
  lineageSQS : sqs.IQueue;
  pipelineOpsSQS : sqs.IQueue;
  documentRegistrySQS : sqs.IQueue;
  metadataTopic : sns.ITopic;
  esDomain: es.IDomain;
}

export class TextractPipelineStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: MultistackProps) {
    super(scope, id, props);

    //**********SNS Topics******************************
    const textractJobCompletionTopic = new sns.Topic(this, 'JobCompletion');

    //**********IAM Roles******************************
    const textractServiceRole = new iam.Role(this, 'TextractServiceRole', {
      assumedBy: new iam.ServicePrincipal('textract.amazonaws.com')
    });
    textractServiceRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        resources: [textractJobCompletionTopic.topicArn],
        actions: ["sns:Publish"]
      })
    );


    //**********S3 Bucket******************************
    //S3 bucket for input documents and output
    const rawContentsBucket = new s3.Bucket(this, 'RawDocumentsBucket', { versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY});

    const asyncdocBucket = new s3.Bucket(this, 'LargeDocumentsBucket', { versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY});

    const syncdocBucket = new s3.Bucket(this, 'ImageDocumentsBucket', { versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY});

    const textractResultsBucket = new s3.Bucket(this, 'TextractResultsBucket', { versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY});

    //Comprehend Output Bucket
    const comprehendResultsBucket = new s3.Bucket(this, 'ComprehendResultsBucket', { versioned: false, removalPolicy: cdk.RemovalPolicy.DESTROY});

    //Queue
    const jobResultsQueue = new sqs.Queue(this, 'JobResults', {
      visibilityTimeout: cdk.Duration.seconds(900), retentionPeriod: cdk.Duration.seconds(1209600)
    });
    //Trigger

    textractJobCompletionTopic.addSubscription(
      new snsSubscriptions.SqsSubscription(jobResultsQueue)
    );
  
    const pipelineLayer = new lambda.LayerVersion(this, 'PipelineFunctionsLayer', {
      code: lambda.Code.fromAsset('code/lambda_layer/pipeline'),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_7],
      license: 'Apache-2.0',
      description: 'NLP Pipeline and Document Registration Layer',
    });

    // S3 Event processor
    const documentRegistrar = new lambda.Function(this, 'DocumentRegistrar', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/document_registrar'),
      handler: 'document_registrar.lambda_handler',
      timeout: cdk.Duration.seconds(30),
      environment: {
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });
    //Layer
    documentRegistrar.addLayers(pipelineLayer)
    //Trigger
    documentRegistrar.addEventSource(new S3EventSource(rawContentsBucket, {
      events: [
        s3.EventType.OBJECT_CREATED,
        s3.EventType.OBJECT_REMOVED_DELETE
      ]
    }));

    //Permissions
    rawContentsBucket.grantReadWrite(documentRegistrar)
    
    documentRegistrar.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );

    const documentClassifier = new lambda.Function(this, 'DocumentClassifier', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/document_classifier'),
      handler: 'document_classifier.lambda_handler',
      timeout: cdk.Duration.seconds(30),
      environment: {
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });
    
    documentClassifier.addLayers(pipelineLayer)
    //Trigger
    documentClassifier.addEventSource(new DynamoEventSource(props.documentRegistryTable, {
      startingPosition: lambda.StartingPosition.TRIM_HORIZON
    }));
    
    documentClassifier.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );

    //------------------------------------------------------------

    // Document processor (Router to Sync/Async Pipeline)
    const extensionDetector = new lambda.Function(this, 'ExtensionDetector', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/extension_detector'),
      handler: 'extension_detector.lambda_handler',
      timeout: cdk.Duration.seconds(900),
      environment: {
        TARGET_SYNC_BUCKET :  syncdocBucket.bucketName,
        TARGET_ASYNC_BUCKET : asyncdocBucket.bucketName,
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });
    //Layer
    extensionDetector.addLayers(pipelineLayer)
    //Trigger
    extensionDetector.addEventSource(new DynamoEventSource(props.pipelineOpsTable, {
      startingPosition: lambda.StartingPosition.TRIM_HORIZON
    }));

    //Permissions`
    rawContentsBucket.grantReadWrite(extensionDetector)
    asyncdocBucket.grantReadWrite(extensionDetector)
    syncdocBucket.grantReadWrite(extensionDetector)
    
    extensionDetector.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );

    //------------------------------------------------------------

    // Sync Jobs Processor (Process jobs using sync APIs)
    const textractSyncProcessor = new lambda.Function(this, 'TextractSyncProcessor', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/textract_sync'),
      handler: 'textract_processor.lambda_handler',
      reservedConcurrentExecutions: 1,
      timeout: cdk.Duration.seconds(25),
      environment: {
        PIPELINE_OPS_TABLE: props.pipelineOpsTable.tableName,
        TARGET_TEXTRACT_BUCKET_NAME: textractResultsBucket.bucketName,
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });
    //Layer
    textractSyncProcessor.addLayers(pipelineLayer)
    //Trigger
    textractSyncProcessor.addEventSource(new S3EventSource(syncdocBucket, {
      events: [ s3.EventType.OBJECT_CREATED ]
    }));
    //Permissions
    syncdocBucket.grantReadWrite(textractSyncProcessor)
    textractResultsBucket.grantReadWrite(textractSyncProcessor)
    textractSyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["textract:*"],
        resources: ["*"]
      })
    );
    textractSyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );
    //------------------------------------------------------------

    // Async Job Processor (Start jobs using Async APIs)
    const textractAsyncStarter = new lambda.Function(this, 'TextractAsyncStarter', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/textract_async'),
      handler: 'textract_starter.lambda_handler',
      reservedConcurrentExecutions: 50,
      timeout: cdk.Duration.seconds(60),
      environment: {
        TEXTRACT_SNS_TOPIC_ARN : textractJobCompletionTopic.topicArn,
        TEXTRACT_SNS_ROLE_ARN : textractServiceRole.roleArn,
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });

    //Layer
    textractAsyncStarter.addLayers(pipelineLayer)
    //Triggers
    textractAsyncStarter.addEventSource(new S3EventSource(asyncdocBucket, {
      events: [ s3.EventType.OBJECT_CREATED ]
    }));
    //Run when a job is successfully complete
    textractAsyncStarter.addEventSource(new SnsEventSource(textractJobCompletionTopic))
    //Permissions
    asyncdocBucket.grantRead(textractAsyncStarter)
    textractAsyncStarter.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["iam:PassRole"],
        resources: [textractServiceRole.roleArn]
      })
    );
    textractAsyncStarter.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["textract:*"],
        resources: ["*"]
      })
    );
    textractAsyncStarter.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );
    //------------------------------------------------------------

    // Async Jobs Results Processor
    const textractAsyncProcessor = new lambda.Function(this, 'TextractAsyncProcessor', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/textract_async'),
      handler: 'textract_processor.lambda_handler',
      memorySize: 10000,
      reservedConcurrentExecutions: 50,
      timeout: cdk.Duration.seconds(900),
      environment: {
        TARGET_TEXTRACT_BUCKET_NAME: textractResultsBucket.bucketName,
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });
    //Layer
    textractAsyncProcessor.addLayers(pipelineLayer)
    //Triggers
    textractAsyncProcessor.addEventSource(new SqsEventSource(jobResultsQueue, {
      batchSize: 1
    }));
    //Permissions
    asyncdocBucket.grantReadWrite(textractAsyncProcessor)
    textractResultsBucket.grantReadWrite(textractAsyncProcessor)
    jobResultsQueue.grantConsumeMessages(textractAsyncProcessor)
    textractAsyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["textract:*"],
        resources: ["*"]
      })
    );
    textractAsyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );

    //--------------

    // Comprehend Lambda
    const comprehendSyncProcessor = new lambda.Function(this, 'ComprehendSyncProcessor', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/comprehend_sync'),
      handler: 'comprehend_processor.lambda_handler',
      memorySize: 10000,
      reservedConcurrentExecutions: 50,
      timeout: cdk.Duration.seconds(900),
      environment: {
        TARGET_ES_CLUSTER: props.esDomain.domainEndpoint,
        TARGET_COMPREHEND_BUCKET: comprehendResultsBucket.bucketName,
        METADATA_SNS_TOPIC_ARN : props.metadataTopic.topicArn
      }
    });
    //Layer
    comprehendSyncProcessor.addLayers(pipelineLayer)
    //Trigger
    comprehendSyncProcessor.addEventSource(new S3EventSource(textractResultsBucket, {
      events: [ s3.EventType.OBJECT_CREATED ],
      filters: [ { suffix: 'fullresponse.json' }]
    }));
    //Permissions
    textractResultsBucket.grantReadWrite(comprehendSyncProcessor)
    comprehendResultsBucket.grantReadWrite(comprehendSyncProcessor)
    comprehendSyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["es:*"],
        resources: ["*"]
      })
    );
    comprehendSyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["comprehend:*"],
        resources: ["*"]
      })
    );
    comprehendSyncProcessor.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["sns:publish"],
        resources: ["*"]
      })
    );
  }
}