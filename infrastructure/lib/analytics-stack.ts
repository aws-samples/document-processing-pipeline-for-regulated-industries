import * as cdk from '@aws-cdk/core';
import es = require('@aws-cdk/aws-elasticsearch');
import { AccountPrincipal, PolicyStatement } from '@aws-cdk/aws-iam';


export class AnalyticsStack extends cdk.Stack {
  public readonly esDomain : es.IDomain;
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const esDomainName = "nlpexampledomain";
    // NLP Elasticsearch Cluster to search Comprehend results
    this.esDomain = new es.Domain(this, 'NLP Elasticsearch Domain', {
      version: es.ElasticsearchVersion.V7_8,
      domainName: esDomainName,
      accessPolicies: [
        new PolicyStatement({
          principals: [new AccountPrincipal(this.account)],
          actions: ["es:*"],
          resources: ["arn:aws:es:"+this.region+":"+this.account+":domain/"+esDomainName+"/*"]
        })
      ],
      ebs: {
          enabled: true,
          volumeSize: 10
      },
      nodeToNodeEncryption: true,
      encryptionAtRest: {
        enabled: true
      },
      enforceHttps: true,
      capacity: {
        dataNodeInstanceType: 'r5.large.elasticsearch',
        dataNodes: 1
      }
    });
}}