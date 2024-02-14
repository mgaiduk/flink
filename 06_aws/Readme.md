This tutorial shows how to make Flink read from AWS Kinesis, and how to set that up locally
## Setting up local kinesis
I use [kinesalite](https://github.com/mhart/kinesalite) to run kinesis locally for debugging purposes. To install and run:   
```bash
npm install -g kinesalite
kinesalite # launches on port 4567
```
`aws-cli` still requires some auth tokens to be passed around, even though `kinesalite` does not, so go through the configure process:   
```bash
aws configure
# AWS Access Key ID: dummy
# AWS Secret Access Key: dummy
# Default region name: us-east-1
# Default output format: json
```
Then, create a stream:   
```bash
# create a stream
aws kinesis  --endpoint-url http://localhost:4567 --region us-east-1 create-stream --stream-name events --shard-count 1
```
List streams to make sure the stream is created:   
```bash
aws kinesis list-streams --endpoint-url http://localhost:4567 --region us-east-1
```
Push some data into stream. Data has to be b64-encoded:
```bash
aws kinesis  --endpoint-url http://localhost:4567 --region us-east-1 put-record --stream-name events --partition-key asd --data 'eyJldmVudE5hbWUiOiJ2aWV3IiwidXNlcklkIjoiMTIzOCIsImNyZWF0b3JJZCI6Ijc3NzYiLCJldmVudFRzIjoxNzA3NjA2NjY3fQo='
```
## Flink code to read from kinesis
To read from kinesis, we use [this](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kinesis/) kinesis connector.   

Set up dependency:   
```Kotlin
implementation("org.apache.flink:flink-connector-kinesis:4.2.0-1.18")
```
Set up kinesis consumer, with some bogus auth tokens and region to make it work:   
```Kotlin
val consumerConfig = Properties()
consumerConfig[AWSConfigConstants.AWS_REGION] = "us-east-1"
consumerConfig[AWSConfigConstants.AWS_ACCESS_KEY_ID] = "dummy"
consumerConfig[AWSConfigConstants.AWS_SECRET_ACCESS_KEY] = "dummy"
consumerConfig[ConsumerConfigConstants.STREAM_INITIAL_POSITION] = "TRIM_HORIZON"
consumerConfig[AWSConfigConstants.AWS_ENDPOINT] = "http://localhost:4567"

val env = StreamExecutionEnvironment.getExecutionEnvironment()

val kinesis: DataStream<String> = env.addSource(FlinkKinesisConsumer(
    "events", SimpleStringSchema(), consumerConfig))
```
The `kinesis` source has a type of `DataStream<String>`, which is not the same as we had before, with kafka (`Source<String, *, *>). This corresponds to the source PLUS the way to read from the source, so we don't have to specify any extra reading methods. But this also means we need to modify the code of the workflow as well:   
```Kotlin
fun defineWorkflow(
    source: DataStream<String>,
    sinkApplier: (stream: DataStream<AggregatedEvent>) -> Unit
) {
    val watermarkStrategy = WatermarkStrategy
        .forMonotonousTimestamps<Event>()
        .withTimestampAssigner { event: Event, _: Long -> 
            event.eventTs * 1000 // convert to ms
        }
    val counts = source
        .flatMap(Tokenizer())
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .name("tokenizer")
        .keyBy { 
            value -> 
                println("Key by userId: ${value.userId}")
                KeyPair(eventName = value.eventName, key = value.userId) 
            }
        .window(TumblingEventTimeWindows.of(WindowTime.seconds(5)))
        .reduce(Sum(), ProcessEvents())
        .name("counter")

    sinkApplier(counts)
}
```
Our `kinesis` source already has a type of `DataStream`, so we can call `flatMap` on it directly.    

Because of kinesalite bug, topic publishing timestamps are incorrectly converted to milliseconds, causing all times to be from somewhere in year 1970. This also affects the window aggregation - it will not be triggered unless 5000 seconds has passed, which can be seen as a "job being stuck". To fix it, I use the timestamps from event json itself. This step shouldn't be necessary when working with the actual aws cloud.