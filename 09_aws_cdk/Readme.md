In the last couple of tutorials, we went through the process of setting up Flink as a Managed Flink Application (formerly, Managed Kinesis Analytics) in AWS. To do that, we had to go to the console and setup all necessary resources: kinesis datastream, dynamodb with necessary schema, Flink app itself, and permissions.   

Creating stuff through the UI has its own pros and cons; it is actually recommended in the very beginning, so that you see everything that happens and can react to problems as they arise. In this tutorial, we'll discuss another option, commonly referred to as "infrastructure as code". CDK, or "Cloud Development Kit", is Amazon way of doing infrastructure as code. Closest analogue to that outside of aws is [Terraform](https://www.terraform.io/).   

Here are potential pros of the approach:
- All resource creation is commited as code, on github. Team attrition will not result in loss of that knowledge
- CDK deployments make sure to keep all resources up-to-date, deleting old resources that are no longer needed. People, on the other hand, tend to leave stuff running in the cloud, especially after one-off experiments.
- It is easy to replicate cdk deployments, copying entire infrastructure stack when necessary. Examples: creating alternative, "staging" environment, or creating alternative infra stack in another zone   

In this tutorial, we will set up Flink application with dependencies (kinesis datastream + dynamodb + permissions) using typescript cdk.
## Getting started with cdk
First, let's set up a cdk project. We will follow [this](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) guide.    

Create a new folder, and install cdk:
```bash
mkdir cdk
cd cdk
npm install -g aws-cdk
```
Next, we init a sample app using `sample-app` template:   
```bash
cdk init sample-app --language typescript
```
`sample-app` is an app that uses SQS queues and SNS topics in AWS. This is not something we need for Flink, but it sets us up with the overall project structure, and we can rewrite the code from there. Here is the directory structure that we get after initializing the app:   
```
./.npmignore
./test
./test/cdk.test.ts
./bin
./bin/cdk.ts
./jest.config.js
./node_modules/*
./cdk.json
./.local_hist
./README.md
./.gitignore
./package-lock.json
./package.json
./lib
./lib/cdk-stack.ts
./tsconfig.json
```
Most of these files are just various configs for npm/js/ts application (package*, node_modules, ts_config.json, jest for testing). `cdk.json` is the config file for the cdk, but we probably won't change anything there.   

`bin/cdk.ts` is an entrypoint to the app:   
```ts
#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CdkStack } from '../lib/cdk-stack';

const app = new cdk.App();
new CdkStack(app, 'CdkStack');
```
It creates a *cdk app*, and a new *stack* called CdkStack attached to it.   

Here is the definition of the CdkStack, in `lib/cdk-stack.ts`:   
```ts
import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, 'CdkQueue', {
      visibilityTimeout: Duration.seconds(300)
    });

    const topic = new sns.Topic(this, 'CdkTopic');

    topic.addSubscription(new subs.SqsSubscription(queue));
  }
}
```
As promised, it only has sqs queue and sns topic.   

Ckd *stack* is a unit of deployment. Technically, you can put all of your resources in one stack; alternatively, you can separate them in different stacks, which allows independant deployment and rollback of these stacks.   

Before philosophising further, let's get this thing deployed so we can take a look at it at AWS console UI. After creating cdk app for the first time, we need to "bootstrap" it:   
```bash
cdk bootstrap --profile my-sso-profile aws://12345678/us-east-1
```
```
 ⏳  Bootstrapping environment aws://767397884728/us-east-1...
Trusted accounts for deployment: (none)
Trusted accounts for lookup: (none)
Using default execution policy of 'arn:aws:iam::aws:policy/AdministratorAccess'. Pass '--cloudformation-execution-policies' to customize.

 ✨ hotswap deployment skipped - no changes were detected (use --force to override)

 ✅  Environment aws://767397884728/us-east-1 bootstrapped (no changes).
 ```
As always, change account number and region as needed.   

`cdk bootstrap`, as well as later commands, requires aws authentication. You can either provide access key and secret token by typical means (config, env variable etc), or use sso profile like this: `--profile my_profile_name`. Use `aws configure sso` first to make that work.   

"Bootstrap" creates cloud resources needed for cdk to function. We can see the newly bootstrapped cdk in the CloudFormation aws console service. CloudFormation is another tool for "infrastructure as code" in aws. It uses yaml or json configs to declare which resources need to be created. Cdk is a tool that operates on top of CloudFormation; it uses an imperative language (like typescript) to define the resources needed, and cdk itself will convert that into CloudFormation config during deployment. You can run `cdk synth` to view the actual config before deploying it.    

![Sigmoid](./cdk.png "Title")    

Now let's deploy our sample app:   
```bash
cdk deploy --profile my-sso-profile
```

```
  Synthesis time: 6.6s

This deployment will make potentially sensitive changes according to your current security approval level (--require-approval broadening).
Please confirm you intend to make the following modifications:

IAM Statement Changes
┌───┬─────────────────┬────────┬─────────────────┬───────────────────────────┬─────────────────────────────────────────────────┐
│   │ Resource        │ Effect │ Action          │ Principal                 │ Condition                                       │
├───┼─────────────────┼────────┼─────────────────┼───────────────────────────┼─────────────────────────────────────────────────┤
│ + │ ${CdkQueue.Arn} │ Allow  │ sqs:SendMessage │ Service:sns.amazonaws.com │ "ArnEquals": {                                  │
│   │                 │        │                 │                           │   "aws:SourceArn": "${CdkTopic}"                │
│   │                 │        │                 │                           │ }                                               │
└───┴─────────────────┴────────┴─────────────────┴───────────────────────────┴─────────────────────────────────────────────────┘
(NOTE: There may be security-related changes not in this list. See https://github.com/aws/aws-cdk/issues/1299)

Do you wish to deploy these changes (y/n)?
```
In my case, it asks for a confirmation when chaning IAM policies.   

Now, in the CloudFormation service in aws console, we can see our newly created stack called `CdkStack`. On the `Resources` tab of the stack, you can see the actual resources that got created, SNS topic for example.
## Creating kinesis datastream with cdk
But we didn't do it for sns topics or sqs queues. We need kinesis datastream, dynamodb and flink. Change the code in the `cdk-stack.ts` to:   
```ts
import { Duration, Stack, StackProps,
  aws_kinesis as kinesis,
} from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const signalStream = new kinesis.Stream(this, 'SignalStream', {
      streamMode: kinesis.StreamMode.ON_DEMAND,
      streamName: `flink-test`,
    });
  }
}

```
We add an import of `aws_kinesis` from `aws-cdk-lib`; then we create a new kinesis stream. The documentation of the function call can be found [here](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_kinesis.Stream.html).   

After running the deployment again, this is what we can see in the console:   
```
CdkStack:  start: Building 01afa4b3cdba34b7d3d86bd21642f26c03d580b849b2e44be1cda674eec69c30:current_account-current_region
CdkStack:  success: Built 01afa4b3cdba34b7d3d86bd21642f26c03d580b849b2e44be1cda674eec69c30:current_account-current_region
CdkStack:  start: Publishing 01afa4b3cdba34b7d3d86bd21642f26c03d580b849b2e44be1cda674eec69c30:current_account-current_region
CdkStack:  success: Published 01afa4b3cdba34b7d3d86bd21642f26c03d580b849b2e44be1cda674eec69c30:current_account-current_region
CdkStack: deploying... [1/1]
CdkStack: creating CloudFormation changeset...
[████████████████████████████████████▎·····················] (5/8)

13:48:02 | UPDATE_COMPLETE_CLEA | AWS::CloudFormation::Stack | CdkStack
13:48:06 | DELETE_IN_PROGRESS   | AWS::SNS::Topic      | CdkTopic7E7E1214
13:48:06 | DELETE_IN_PROGRESS   | AWS::SQS::Queue      | CdkQueueBA7F247D
```
This demonstrates an important cdk/cloudformation concept - *changesets*. A cdk *stack* is like an umbrella that groups together a bunch of aws resources under a common name. After changing the code and triggering the deployment, cdk generates a new CloudFormation template that only has a kinesis datastream defined in it. Old template had SNS topic and SQS queue. After the deployment, SNS topic and SQS queue will be deleted, and kinesis datastream will be created. If you run the deployment again, it will not create a second datastream; CloudFormation sees that you need one datastream, and that one datastream already exists, so no resource changes will be made.   

Now in the Resources tab of the cloudformation stack, you can see the resource for Kinesis Datastream. You can click on the link, get to the Kinesis Datastream page on Kinesis aws service, and see for yourself that a datastream created from cdk is exactly the same as a datastream created in AWS Console UI.
## Adding more resources for Flink
Now, lets add a dynamodb:  
```ts
import { ...,
  aws_dynamodb as dynamodb,
  RemovalPolicy,
} from 'aws-cdk-lib';

...

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    ...

    const dynamodbSink = new dynamodb.Table(this, 'community-feed-table', {
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
  }
}

```
Just as with aws cli/console creation, we have to specify primary and secondary keys, table name, ttl attribute and so on.   

It is a good idea to trigger the deployment each time you make a change, to catch potential problems early on.   

Next, let's add roles with necessary permissions:   
```ts
import {...,
  aws_iam as iam,
} from 'aws-cdk-lib';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {

    ...

    const flinkRole = new iam.Role(this, 'ApplicationRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      roleName: `flink-cdk-test-role`,
    });

    dynamodbSink.grantWriteData(flinkRole);
    signalStream.grantRead(flinkRole)
  }
}
```
If you recall, for AWS Console UI permissions, we had to go through a laborious process of remembering different resource arns, modifying the config in different places and so on. Now, we just tell datastream and dynamodb to grant a permission to flink role. Elegant!   

Next, let's create the actual flink app. First, install flink library using npm: 
```bash
npm install @aws-cdk/aws-kinesisanalytics-flink-alpha
```
And add the code to initialize Flink app: 
```ts
import { ...
  aws_logs as logs,
} from 'aws-cdk-lib';
import * as flink from '@aws-cdk/aws-kinesisanalytics-flink-alpha';
import path from 'path';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    ...

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
```
Before, we had to manually build a jar file, upload it to s3, and then point our Flink app to it. With cdk, we can just point the config to our local `.jar` file, the rest will be handled for us. We do need to build the jar ourselves though; with development teams, that part is usually automated using ci/cd.   

Because of the `path` module, you might stumble on the following error:
```
  node_modules/@types/node/path.d.ts:178:5
    178     export = path;
            ~~~~~~~~~~~~~~
    This module is declared with 'export =', and can only be used with a default import when using the 'esModuleInterop' flag.
```
To fix, insert the following into `tsconfig.json`:
```json
"esModuleInterop": true,
```
Deploying this will create Managed Apache Flink application, dynamodb table, kinesis datastream, and all required roles and permissions.   

We only have a couple of problems now, though not critical:
- The application being deployed does not run automatically. We still have to start it manually in AWS Console, though just the first time. This can be automated using Lambdas.
- We have some configurations, like datastream name and dynamodb table name, that are duplicated in cdk and in Flink yaml config. If we had many environments (prod/staging, for example), it might get messy. We can unite these by using FlinkApplicationProperties in cdk ([reference](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-kinesisanalytics-flink-alpha-readme.html)). They allow passing arbitrary configuration externally via cdk, and then reading that in Flink code.
## Potential problems
Sometimes, you might see a cdk deployment error like this:
```
Failed to take snapshot for the application
```
Or this:
```
UPDATE_ROLLBACK_FAILED
```
That might happen due to some exceptions in Flink. Normally, Flink reads data from the input stream, processes it, writes to the sink, and snapshots its state once every minute. If it encounters an exception - permissions misconfiguration, for example, or one unexpected record that has wrong format - Flink stops making progress, and hence, snapshots. This is the default behaviour; there is a trade-off between uptime and correctness in such case. As an alternative, Flink could've just skip bad records; no processing downtime will occur, but if suddenly 100% of records become "bad", all the data will be lost, and noone even will be alerted.   

On the other hand, cdk assumes that Flink has important information in its state, and waits for it to make a snapshot before any changes, like fresh deployment or even rollback, can be made. Unfortunately, cdk is not smart enough to track that Flink is not actually making any progress, and it just should use last successful snapshot.   

In such a case, cdk deployment will get stuck. It will try to deploy new version; it won't work; cdk will try to revert the deployment to the last correct version, but the rollback will also not work because of the snapshotting problem. The system will get stuck and will require some manual intervention:
- Open Flink app in UI and force-stop it. 
- Run the following command to perform the rollback and clear the error:   
```bash
aws cloudformation --profile my-sso-profile continue-update-rollback --stack-name CdkStack
```
After this, cdk will get "unstuck" and further deployments will be possible.