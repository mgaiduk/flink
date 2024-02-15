In this tutorial, we'll discuss how to setup cross account permissions and how to modify the code for that to work.   

In previous tutorials, when launching something in aws cloud, all resources (source, sink + flink app) were in the same account. We didn't even specify the account to which the resource belonged; for kinesis stream, for example, we just specified stream name, and the default behaviour of aws is to assume that it is in the same account.    

We'll now setup permissions and configuration to read from kinesis datastream in a different account. We will assume that flink app and sink are in the same account. We will refer to these accounts as "app account" for flink + dynamodb sink, and "source" account for the kinesis source. The steps to do will be:   
- Set up role in source account with kinesis datastream permissions
- Set up another permission for the app role - to assume role in source account
- Add trusted relationship between source account role and app account role
- Add configuration for the assumed role in the flink kinesis source code
## Setting up a role in a source account
First, create a kinesis datastream (`flink-test`) if you haven't already.   

Then, create IAM policy in source account with the following content:   
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadInputStream",
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
                "kinesis:ListShards"
            ],
            "Resource": 
               "arn:aws:kinesis:us-west-2:123456768:stream/flink-test"
        }
    ]
}
```
Modify region and account name accordingly. Name this policy `KA-Source-Stream-Policy`.    

Next, create a role in source account that will use this policy:   
IAM -> create role -> AWS service -> Kinesis/Kinesis Analytics use case
Add `KA-Source-Stream-Policy` to permissions;   
Name the role `KA-Source-Stream-Role`; note the `arn` id of the role: `arn:aws:iam::123456768:role/KA-Source-Stream-Role`   

## Setting up permission to assume role in the app account
Go to the role in the app account associated with the flink app. You can see it right on the managed flink dashboard. Edit the policy of the role, and add the following section:   
```json
{
    "Sid": "AssumeRoleInSourceAccount",
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": "arn:aws:iam::12345678:role/KA-Source-Stream-Role"
},
```
Replace acc number with the number for the source account.   
## Add trusted relationship between source account and app account
Go to the source account again, open the `KA-Source-Stream-Role`.   
Trust relationships -> Edit trust policy, and add the following content:   
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::87654321:role/service-role/kinesis-analytics-flink-test-us-east-1"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```
Now replace the acc number and role to the one corresponding to *app* account.
## Modifying the code
Create the following config:   
```yaml
source:
  awsRegion: "us-east-1"
  streamInitialPosition: "LATEST"
  datastreamName: "flink-test"
  awsAssumeRoleArn: "arn:aws:iam::12345678:role/KA-Source-Stream-Role"
sink:
  awsRegion: "us-east-1"
  tableName: "flink-test"
```
The only difference here is the `awsAssumeRoleArn` which now contains the `arn` of the *source account role*.   

Modify the config class definitions to include this role arn:   
```Kotlin
data class SourceConfig(
    val awsEndpoint: String?,
    val awsRegion: String?,
    val awsAccessKeyId: String?,
    val awsSecretAccessKey: String?,
    val streamInitialPosition: String = "TRIM_HORIZON",
    val datastreamName: String,
    val kinesaliteTimestampFix: Boolean = false,
    val awsAssumeRoleArn: String?,
)
```
We make it an optional string; if no assume role arn is passed, we will operate from the role of the current account.   

Next, modify the kinesis source initialization code:   
```Kotlin
fun initKinesisSource(config: SourceConfig): FlinkKinesisConsumer<String> {
    val properties = Properties()
    ...
    if (config.awsAssumeRoleArn != null) {
        properties[AWSConfigConstants.AWS_CREDENTIALS_PROVIDER] = "ASSUME_ROLE"
        properties[AWSConfigConstants.AWS_ROLE_ARN] = config.awsAssumeRoleArn
        properties[AWSConfigConstants.AWS_ROLE_SESSION_NAME] = "ksassumedrolesession"
    }
    ...
}
```
We add credentials provider, role arn and session name (can be anything, as far as I know) to the source configuration.