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


import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

const val TOPIC = "input"
const val DECAY_INTERVAL: Long = 5

@Serializable
data class RawEvent(
    val eventName: String = "",
    val userId: String = "",
    val creatorId: String = "",
    val eventTs: Long = 0,
)

data class Event(
    val eventName: String = "",
    val userId: String = "",
    val creatorId: String = "",
    val eventTs: Long = 0,
    var eventCount: DecayCounter = DecayCounter(DECAY_INTERVAL, 0, 0.0),
)

@Serializable
data class DecayCounter(
    val decayInterval: Long,
    var lastUpdate: Long,
    var value: Double,
) {
    operator fun plus(other: DecayCounter): DecayCounter {
        if (other.decayInterval != this.decayInterval) {
            throw RuntimeException("Attempt to add decay counters with different decay interval")
        }
        var biggest = this
        var smallest = other
        if (other.lastUpdate > this.lastUpdate) {
            biggest = other
            smallest = this
        }
        var delta = biggest.lastUpdate - smallest.lastUpdate
        var ratio = delta.toDouble() / this.decayInterval
        var decayedValue = smallest.value * kotlin.math.exp(-ratio)
        println("Ratio: ${ratio}, decayedValue: ${decayedValue}")
        return DecayCounter(decayInterval = this.decayInterval, lastUpdate = biggest.lastUpdate, value = biggest.value + decayedValue)
    }
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
    // checkpoint every minute
    env.enableCheckpointing(60000);

    // configure dynamodb sink
    var sinkProperties = Properties();
    sinkProperties.put(AWSConfigConstants.AWS_REGION, "ap-south-2");
    sinkProperties.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:8000");

    val dynamoDbSink = DynamoDbSink.builder<Event>()
        .setTableName("UserCounters")
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("userId"))
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
        .keyBy { value -> value.userId }
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce(Sum(), ProcessEvents())
        .name("counter")

    sinkApplier(counts)
}

class Tokenizer : FlatMapFunction<String, Event> {
    override fun flatMap(line: String, out: Collector<Event>) {
        val json = Json { ignoreUnknownKeys = true } // Create a Json instance with configuration
        val rawEvent = json.decodeFromString(RawEvent.serializer(), line)
        val event = Event(eventName = rawEvent.eventName, userId = rawEvent.userId, creatorId = rawEvent.creatorId, 
            eventCount = DecayCounter(decayInterval = DECAY_INTERVAL, lastUpdate = rawEvent.eventTs, value = 1.0))
        println("Decoded event from json: ${rawEvent}")
        out.collect(event)
    }
}

class Sum : ReduceFunction<Event> {
    override fun reduce(value1: Event, value2: Event): Event {
        value1.eventCount += value2.eventCount
        return value1
    }
}

class ProcessEvents :
    ProcessWindowFunction<Event, Event, String, TimeWindow>() {
    
    private lateinit var stateDescriptor: ValueStateDescriptor<Event>

    override fun open(parameters: Configuration) {
        stateDescriptor = ValueStateDescriptor("Event", Event::class.java)
        super.open(parameters)
    }

    override fun process(
        userId: String,
        context: Context,
        events: Iterable<Event>,
        collector: Collector<Event>
    ) {
        val state: ValueState<Event> = context.globalState().getState(stateDescriptor)

        var accumulatedEvent = state.value() ?: Event(userId = userId, eventCount = DecayCounter(DECAY_INTERVAL, 0, 0.0))
        for (event in events) {
            accumulatedEvent.eventCount += event.eventCount
        }

        println("accumulatedEvent: ${accumulatedEvent}")
        state.update(accumulatedEvent)
        accumulatedEvent.let { collector.collect(it) }
    }
}

/** Example DynamoDB element converter. */
class CustomElementConverter : ElementConverter<Event, DynamoDbWriteRequest> {

    override fun apply(event: Event, context: SinkWriter.Context): DynamoDbWriteRequest {
        val item = hashMapOf<String, AttributeValue>(
            "userId" to AttributeValue.builder().s(event.userId).build(),
            "count" to AttributeValue.builder().n(event.eventCount.value.toString()).build()
        )

        return DynamoDbWriteRequest.builder()
            .setType(DynamoDbWriteRequestType.PUT)
            .setItem(item)
            .build()
    }
}