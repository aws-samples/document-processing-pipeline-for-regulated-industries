import * as cdk from '@aws-cdk/core';
import { SqsEventSource } from '@aws-cdk/aws-lambda-event-sources';
import sns = require('@aws-cdk/aws-sns');
import snsSubscriptions = require("@aws-cdk/aws-sns-subscriptions");
import sqs = require('@aws-cdk/aws-sqs');
import dynamodb = require('@aws-cdk/aws-dynamodb');
import lambda = require('@aws-cdk/aws-lambda');

export class MetadataStack extends cdk.Stack {
  public readonly pipelineOpsTable : dynamodb.Table;
  public readonly lineageTable : dynamodb.Table;
  public readonly indexName : string;
  public readonly documentRegistryTable : dynamodb.Table;
  public readonly lineageSQS : sqs.IQueue;
  public readonly pipelineOpsSQS : sqs.IQueue;
  public readonly documentRegistrySQS : sqs.IQueue;
  public readonly metadataTopic : sns.Topic;
  
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Metadata SNS Topic
    this.metadataTopic = new sns.Topic(this, 'MetadataTopic', {
      fifo: true, contentBasedDeduplication : true, topicName : 'MetadataServicesTopic'
   });
  
    // SQS Queues for consuming metadata topic
    this.lineageSQS = new sqs.Queue(this, 'Document Lineage SQS', {
      fifo: true, visibilityTimeout: cdk.Duration.seconds(30), retentionPeriod: cdk.Duration.minutes(60)
   });

    this.pipelineOpsSQS = new sqs.Queue(this, 'Pipeline Ops SQS', {
      fifo: true, visibilityTimeout: cdk.Duration.seconds(30), retentionPeriod: cdk.Duration.minutes(60)
   });
   
   this.documentRegistrySQS = new sqs.Queue(this, 'Document Registry SQS', {
      fifo: true, visibilityTimeout: cdk.Duration.seconds(30), retentionPeriod: cdk.Duration.minutes(60)
   });
   
   // SNS SQS Subscription Filters
   this.metadataTopic.addSubscription(new snsSubscriptions.SqsSubscription(this.lineageSQS, {
      filterPolicy: {
        metadataType: sns.SubscriptionFilter.stringFilter({
            whitelist: ['document-lineage']
    })
    }}));
    
   this.metadataTopic.addSubscription(new snsSubscriptions.SqsSubscription(this.documentRegistrySQS, {
      filterPolicy: {
        metadataType: sns.SubscriptionFilter.stringFilter({
            whitelist: ['document-registry']
    })
    }}));

   this.metadataTopic.addSubscription(new snsSubscriptions.SqsSubscription(this.pipelineOpsSQS, {
      filterPolicy: {
        metadataType: sns.SubscriptionFilter.stringFilter({
          whitelist: ['pipeline-operations']
      })
    }}));
   
        //**********DynamoDB Table*************************

    //DynamoDB table with links to output in S3
    this.pipelineOpsTable = new dynamodb.Table(this, 'PipelineOpsTable', {
      partitionKey: { name: 'documentId', type: dynamodb.AttributeType.STRING },
      stream: dynamodb.StreamViewType.NEW_IMAGE,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const indexName = "DocumentSignatureIndex";
    
    //DynamoDB table with links to output in S3
    this.lineageTable = new dynamodb.Table(this, 'DocumentLineageTable', {
      partitionKey: { name: 'documentId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'timestamp', type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
    
    this.lineageTable.addGlobalSecondaryIndex({
      indexName: indexName,
      partitionKey: { name: 'documentSignature', type: dynamodb.AttributeType.STRING },
      sortKey:  { name: 'timestamp', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.KEYS_ONLY
    });
    
    this.documentRegistryTable = new dynamodb.Table(this, 'DocumentRegistryTable', {
      partitionKey: { name: 'documentId', type: dynamodb.AttributeType.STRING },
      stream: dynamodb.StreamViewType.NEW_IMAGE,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    
    //**********Lambda Functions******************************

    /// Helper Layer with metadata services functions
    const metadataserviceLayer = new lambda.LayerVersion(this, 'MetadataServicesLayer', {
      code: lambda.Code.fromAsset('code/lambda_layer/metadata-services'),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_7],
      license: 'Apache-2.0',
      description: 'Metadata Services Helper layer.',
    });

    const documentLineageFunction = new lambda.Function(this, 'DocumentLineageFunction', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/metadata'),
      handler: 'lineage.lambda_handler',
      reservedConcurrentExecutions: 50,
      timeout: cdk.Duration.seconds(30),
      environment: {
        DOCUMENT_LINEAGE_TABLE: this.lineageTable.tableName,
        DOCUMENT_LINEAGE_INDEX: indexName,
        SQS_QUEUE_ARN: this.lineageSQS.queueArn
      }
    });
    //Layer
    documentLineageFunction.addLayers(metadataserviceLayer)
    //Triggers
    documentLineageFunction.addEventSource(new SqsEventSource(this.lineageSQS, {
      batchSize: 1
    }));
    //Permissions
    this.lineageTable.grantReadWriteData(documentLineageFunction)
    this.lineageSQS.grantConsumeMessages(documentLineageFunction)
    //------------------------------------------------------------

    // Pipeline Metadata Function
    const pipelineOpsFunction = new lambda.Function(this, 'PipelineOperationsFunction', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/metadata'),
      handler: 'pipeline.lambda_handler',
      reservedConcurrentExecutions: 50,
      timeout: cdk.Duration.seconds(30),
      environment: {
        PIPELINE_OPS_TABLE: this.pipelineOpsTable.tableName,
        SQS_QUEUE_ARN: this.pipelineOpsSQS.queueArn
      }
    });
    //Layer
    pipelineOpsFunction.addLayers(metadataserviceLayer)
    //Triggers
    pipelineOpsFunction.addEventSource(new SqsEventSource(this.pipelineOpsSQS, {
      batchSize: 1
    }));
    //Permissions
    this.pipelineOpsTable.grantReadWriteData(pipelineOpsFunction)
    this.pipelineOpsSQS.grantConsumeMessages(pipelineOpsFunction)
    
    const documentRegistryFunction = new lambda.Function(this, 'DocumentRegistrationFunction', {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset('code/metadata'),
      handler: 'registry.lambda_handler',
      reservedConcurrentExecutions: 50,
      timeout: cdk.Duration.seconds(30),
      environment: {
        REGISTRY_TABLE: this.documentRegistryTable.tableName,
        SQS_QUEUE_ARN: this.documentRegistrySQS.queueArn
      }
    });
    //Layer
    documentRegistryFunction.addLayers(metadataserviceLayer)
    //Triggers
    documentRegistryFunction.addEventSource(new SqsEventSource(this.documentRegistrySQS, {
      batchSize: 1
    }));
    //Permissions
    this.documentRegistryTable.grantReadWriteData(documentRegistryFunction)
    this.documentRegistrySQS.grantConsumeMessages(documentRegistryFunction)

  }}
 