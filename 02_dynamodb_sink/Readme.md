So, we've got our simplest flink setup working. However, printing results in debug log of our cluster's jobmanager is neither impressive nor useful. Let's try to sink data into something tangible. In this example, we will do it with [DynamoDB](https://aws.amazon.com/dynamodb/) - amazon's KV nosql storage.
## Running dynamodb locally
Since we already run docker, running local dynamodb is easy: just add these lines to your `docker-compose.yml` file, to `services` section:  
```
dynamodb-local:
    command: "-jar DynamoDBLocal.jar -sharedDb -dbPath ./data"
    image: "amazon/dynamodb-local:latest"
    container_name: dynamodb-local
    ports:
      - "8000:8000"
    volumes:
      - "./docker/dynamodb:/home/dynamodblocal/data"
    working_dir: /home/dynamodblocal
```
And then restart the docker compose.  

It's also useful to have `aws-cli` installed so that we can deal with our tables outside Flink (Flink might be tough to debug!). [Here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) is the official guide.   

If you don't have aws account, you need something extra to make `aws-cli` work with your local dynamodb:   
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb list-tables --endpoint-url http://localhost:8000
```
We provide fake region, id and access key to silence aws-cli complaining; and we provide our custom `--endpoint-url`.   


Finally, let's create a table:  
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb create-table \
    --table-name WordCounts \
    --attribute-definitions \
        AttributeName=word,AttributeType=S \
    --key-schema AttributeName=word,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
		--endpoint-url http://localhost:8000
```
We use `aws-cli` to create a table in our local dynamodb; we use "word" string as partition keys for KV storage. We don't specify anything else - value being put in there can be anything.
## Adding dynamodb connector to flink
There is an official [dynamodb sink connector](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/dynamodb/) for Flink. To make that doc page work, I had to: 
- Translate maven import code into gradle-kotlin
- Translate java example code into kotlin (chatgpt is really good at this!)
- Lookup missing code: dependency import and implementing custom element converter. Luckily, I found [this example](https://github.com/apache/flink-connector-aws/blob/05fdf377c8846cb140ffa3ec96e8ae07282ef4ce/flink-connector-aws/flink-connector-dynamodb/src/test/java/org/apache/flink/connector/dynamodb/sink/examples/SinkIntoDynamoDb.java#L22) in the official repo   

To go ahead with dynamodb, you can just read further. But be prepared - you will need to do these steps every time your are working with something new!   

Add this to `dependencies` section in `build.gradle.kts`:  
```Kotlin
implementation("org.apache.flink:flink-connector-dynamodb:4.2.0-1.17")
```
Add these imports to `App.kt`: 
```Kotlin
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
```
Next, we define our custom ElementConverter - the class that defines how to convert our Event class, storing words and counts, into something that can be written into dynamodb. Put this somewhere in your `App.kt` file: 
```Kotlin
/** Example DynamoDB element converter. */
class CustomElementConverter : ElementConverter<Event, DynamoDbWriteRequest> {

    override fun apply(event: Event, context: SinkWriter.Context): DynamoDbWriteRequest {
        val item = hashMapOf<String, AttributeValue>(
            "word" to AttributeValue.builder().s(event.word).build(),
            "count" to AttributeValue.builder().n(event.count.toString()).build()
        )

        return DynamoDbWriteRequest.builder()
            .setType(DynamoDbWriteRequestType.PUT)
            .setItem(item)
            .build()
    }
}
```
Template argument `Event` specifies input element type. Then we convert our Event class to hashmap with "word" and "count" values; note that we had to convert number to string here - this is how DynamoDB works. "word" key is necessary here because we specified it as a partition key for our table. The rest is optional, we could omit or rename our fields as we'd like - DynamoDB is a no-sql database, so you are free to save anything in there, you don't have to worry about schema.  

Finally, modify main `runJob` code to use this new sink:  
```Kotlin
fun runJob() {
    val source = KafkaSource.builder<String>()
        .setBootstrapServers("localhost:19092")
        .setTopics(TOPIC)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // NEW CODE
    // configure dynamodb sink
    var sinkProperties = Properties();
    sinkProperties.put(AWSConfigConstants.AWS_REGION, "ap-south-2");
    sinkProperties.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:8000");

    val dynamoDbSink = DynamoDbSink.builder<Event>()
        .setTableName("WordCounts")
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setDynamoDbProperties(sinkProperties)
        .build()



    defineWorkflow(env, source, 1) { workflow -> workflow.sinkTo(dynamoDbSink) }
    env.execute()
}
```
This time, we had to put another bogus region - "ap-south-2". Sink connector doesn't allow arbitrary region names, so I chose one from existing names at random. We specify endpoint for dynamodb, table name, and element converter.   

Build the job, run it, and publish some records into kafka. You can query your table with `aws-cli` like this (query for specific word):   
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb get-item --endpoint-url http://localhost:8000 --table-name WordCounts --key '{"word": {"S": "hello"}}'
```
```
{
    "Item": {
        "count": {
            "N": "1"
        },
        "word": {
            "S": "hello"
        }
    }
}
```
From time to time, you might get the following exception:   
```
Provided list of item keys contains duplicates (Service: DynamoDb, Status Code: 400, Request ID: be728f54-afa8-40fb-8407-a9fbfd62ffe9)
```
This happens because of aggregation window and sink write batch are out of sync. We have 5 second tumbling window aggregation; this will produce 1 record per key every 5 seconds (of kafka timestamp time, not real life time). Our sink write batch size was 20, meaning that it can wait for some values to be aggregated, and attempt to write them all at once. This might cause duplicates to be there.   

Sink builder supports a special attribute to help with that:   
```Kotlin
.setOverwriteByPartitionKeys(listOf("word"))
```
It will cause dynamoDbSink to throw away duplicate entries with the same primary partition key; only the last entry will be left in place.   

Another thing you will notice if you play around with this a bit is that counts reset once in a while. The "aggregation" node in our workflow might create 1 entry per key every 5 seconds; there is no persistent aggregation state between windows; each such entry rewrites previous entries in dynamodb.   

This problem is fatal, and it underlines necessity for some of the most important concepts of Flink. There are 2 potential solutions:   
- Use DynamoDB update expressions. Current official sink connector doesn't support this unfortunately; it is relatively easy to implement one yourself. Main downside of this approach is no "exactly once" guarantee: exceptions in flink jobs might cause it to restart from latest checkpoint and apply the same modification twice.
- Use Flink's KeyedState, stateful streaming and checkpointing mechanism.   

We will use the second approach to fix our problem in the next tutorial.

