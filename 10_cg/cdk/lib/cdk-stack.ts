import { Duration, Stack, StackProps,
  aws_kinesis as kinesis,
  aws_dynamodb as dynamodb,
  RemovalPolicy,
  aws_iam as iam,
  aws_logs as logs,
} from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import * as flink from '@aws-cdk/aws-kinesisanalytics-flink-alpha';
import path from 'path';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const signalStream = new kinesis.Stream(this, 'SignalStream', {
      streamMode: kinesis.StreamMode.ON_DEMAND,
      streamName: `flink-test`,
    });

    const dynamodbSink = new dynamodb.Table(this, 'features-table', {
      tableName: `flink-test`,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,

      partitionKey: {
        name: 'key',
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: 'featureName',
        type: dynamodb.AttributeType.STRING,
      },

      removalPolicy: RemovalPolicy.DESTROY,

      timeToLiveAttribute: 'ttl',

      pointInTimeRecovery: false,
    });

    const dynamodbCGSink = new dynamodb.Table(this, 'cg-table', {
      tableName: `flink-test-cg`,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,

      partitionKey: {
        name: 'CGName',
        type: dynamodb.AttributeType.STRING,
      },

      removalPolicy: RemovalPolicy.DESTROY,

      timeToLiveAttribute: 'ttl',

      pointInTimeRecovery: false,
    });

    const flinkRole = new iam.Role(this, 'ApplicationRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      roleName: `flink-cdk-test-role`,
    });

    dynamodbSink.grantWriteData(flinkRole);
    dynamodbCGSink.grantWriteData(flinkRole);
    signalStream.grantRead(flinkRole);

    const projectRoot = __dirname.split('/lib')[0];

    const code = flink.ApplicationCode.fromAsset(
      path.join(projectRoot, '../app/build/libs/app.jar'),
    );

    new flink.Application(this, 'Application', {
      code,
      runtime: flink.Runtime.FLINK_1_15,
      applicationName: `flink-cdk-test-flink`,
      autoScalingEnabled: true,
      checkpointingEnabled: true,
      checkpointInterval: Duration.millis(60_000),
      minPauseBetweenCheckpoints: Duration.millis(5_000),
      logGroup: new logs.LogGroup(this, 'LogGroup'),
      logLevel: flink.LogLevel.INFO,
      metricsLevel: flink.MetricsLevel.APPLICATION,
      parallelism: 1,
      parallelismPerKpu: 1,
      propertyGroups: {},
      removalPolicy: RemovalPolicy.RETAIN,
      role: flinkRole,
      snapshotsEnabled: true,
    });
  }
}
