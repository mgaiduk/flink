@file:JvmName("WordCount")

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.connector.source.Source
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.PrintSink
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import java.util.Properties

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;


const val TOPIC = "input"

data class Event(var word: String, var count: Int) {
    constructor() : this("", 0)
}

fun main() {
    runJob()
}

fun runJob() {
    val source = KafkaSource.builder<String>()
        .setBootstrapServers("localhost:19092")
        .setTopics(TOPIC)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // configure dynamodb sink
    var sinkProperties = Properties();
    sinkProperties.put(AWSConfigConstants.AWS_REGION, "ap-south-2");
    sinkProperties.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:8000");

    val dynamoDbSink = DynamoDbSink.builder<Event>()
        .setTableName("WordCounts")
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("word"))
        .setDynamoDbProperties(sinkProperties)
        .build()



    defineWorkflow(env, source, 1) { workflow -> workflow.sinkTo(dynamoDbSink) }
    env.execute()
}

fun defineWorkflow(
    env: StreamExecutionEnvironment,
    source: Source<String, *, *>,
    sourceParallelism: Int,
    sinkApplier: (stream: DataStream<Event>) -> Unit
) {
    val textLines = env.fromSource(
        source,
        WatermarkStrategy.forMonotonousTimestamps(),
        "Words"
    ).setParallelism(sourceParallelism)

    val counts = textLines
        .flatMap(Tokenizer())
        .name("tokenizer")
        .keyBy { value -> value.word }
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce(Sum())
        .name("counter")

    sinkApplier(counts)
}

class Tokenizer : FlatMapFunction<String, Event> {
    override fun flatMap(line: String, out: Collector<Event>) {
        line.lowercase()
            .split("\\W+".toRegex())
            .forEach { word ->
                if (!word.isEmpty()) {
                    out.collect(Event(word, 1))
                }
            }
    }
}

class Sum : ReduceFunction<Event> {
    override fun reduce(value1: Event, value2: Event): Event {
        return Event(value1.word, value1.count + value2.count)
    }
}

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