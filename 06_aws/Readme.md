Now, let's run apache flink in AWS cloud!   

There are 2 ways of doing that. You can go the hardcore way, provisioning your own kubernetes cluster and setting up Flink on top of that. In this case, you get actual machines from Amazon, but all the infrastructure handling is on you.     

Alternatively, use "Managed Apache Flink" functionality in AWS. This way, you can upload your jobs directly to this managed flink; control cluster settings in AWS UI or cli; and you don't have to think about servers and kubernetes and stuff like that. The cluster will autoscale, handle checkpointing and logging for you.   

## Setting stuff up in the AWS console
In AWS, IAM permission procedures will differ depending on whether you access resources (flink to kinesis datastream, flink to dynamodb) of the same account or different accounts. Below are the instructions for the "same account" flow.   

Go to aws console -> "Managed Apache Flink" -> Create Streaming Application. Choose flink version (maximum supported is 1.15), choose app name (`flink-test`), and choose a settings template (debug/prod).   

Then create Kinesis Data Stream: Amazon Kinesis -> Data streams -> Create data stream. Choose name (`flink-test`), capacity mode (I chose "on demand"), and settings (I just left all on default).   

Create dynamodb table. Choose name (`flink-test`), partition key (`key`), sort key (`featureName`), settings (default settings).   

Then go to your managed flink app and the IAM role associated with it, then down to permission for that role. Click on "edit" and add the sections for Kinesis Stream access:   
```json
{
    "Sid": "ReadInputStream",
    "Effect": "Allow",
    "Action": "kinesis:*",
    "Resource": "arn:aws:kinesis:us-east-1:acc_number:stream/flink-test"
},
```
Replace the region (`us-east-1`) if necessary. Replace `acc_number` with account number that was used to create the app; you can look it up on the upper right corner of the aws console UI. Replace topic name (`flink-test`) if needed.   

Then set up dynamodb access:   
```json
{
    "Sid": "ListAndDescribe",
    "Effect": "Allow",
    "Action": [
        "dynamodb:List*",
        "dynamodb:DescribeReservedCapacity*",
        "dynamodb:DescribeLimits",
        "dynamodb:DescribeTimeToLive"
    ],
    "Resource": "*"
},
{
    "Sid": "SpecificTable",
    "Effect": "Allow",
    "Action": [
        "dynamodb:BatchGet*",
        "dynamodb:DescribeStream",
        "dynamodb:DescribeTable",
        "dynamodb:Get*",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchWrite*",
        "dynamodb:CreateTable",
        "dynamodb:Delete*",
        "dynamodb:Update*",
        "dynamodb:PutItem"
    ],
    "Resource": "arn:aws:dynamodb:*:*:table/flink-test"
```
Replace table name (`flink-test`) if necessary.   

Finally, create an s3 bucket if you don't have any. It will be used to store the actual `.jar` file of the job.   
## Change the code
In the code, remove references to `AWS_SECRET_ACCESS_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT` from both source and sink. These will be provided by the cluster. This is how the definition of the source should look like:   
```Kotlin
val consumerConfig = Properties()
consumerConfig[AWSConfigConstants.AWS_REGION] = "us-east-1"
consumerConfig[ConsumerConfigConstants.STREAM_INITIAL_POSITION] = "TRIM_HORIZON"
val kinesisSource: DataStream<String> = env.addSource(FlinkKinesisConsumer(
    "flink-test", SimpleStringSchema(), consumerConfig))
```
And for the sink:   
```Kotlin
var sinkProperties = Properties();
    sinkProperties.put(AWSConfigConstants.AWS_REGION, "us-east-1");

    val dynamoDbSink = DynamoDbSink.builder<AggregatedEvent>()
        .setTableName("flink-test")
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("key"))
        .setDynamoDbProperties(sinkProperties)
        .build()
```
Next, change the library version of flink that we are using to `1.15`:   
```Kotlin
implementation("org.apache.flink:flink-clients:1.15.0")
```
Build the jar, upload it to s3. Open the `flink-test` managed flink application; Configure -> set application code location to point to the s3 location of your `.jar`, and click "save changes". In the future, every time you click on "configure" of the already running application and save changes, it will redeploy the application; this will trigger code update and checkpoint load. Always take close attention to the sort of changes you are making. If the changes affect the way we load stuff from the state, migration might be needed.   

Then, press "run" if the application is not yet running. This will start the job, and you can check the status in the flink dashboard. The application will run forever unless cancelled.   
