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
import DECAY_INTERVALS

const val TOPIC = "input"
val DECAY_INTERVALS = longArrayOf(5, 10, 15)

@Serializable
data class Event(
    val eventName: String = "",
    val userId: String = "",
    val creatorId: String = "",
    val eventTs: Long = 0,
    var eventCount: Long = 0,
)

data class AggregatedEvent(
    val eventName: String = "",
    val userId: String = "",
    val creatorId: String = "",
    val eventTs: Long = 0,
    var eventCounts: HashMap<Long, DecayCounter> = hashMapOf()
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

    val dynamoDbSink = DynamoDbSink.builder<AggregatedEvent>()
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
    sinkApplier: (stream: DataStream<AggregatedEvent>) -> Unit
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
        val event = json.decodeFromString(Event.serializer(), line)
        event.eventCount = 1
        println("Decoded event from json: ${event}")
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
    ProcessWindowFunction<Event, AggregatedEvent, String, TimeWindow>() {
    
    private lateinit var stateDescriptor: ValueStateDescriptor<AggregatedEvent>

    override fun open(parameters: Configuration) {
        stateDescriptor = ValueStateDescriptor("AggregatedEvent", AggregatedEvent::class.java)
        super.open(parameters)
    }

    override fun process(
        userId: String,
        context: Context,
        events: Iterable<Event>,
        collector: Collector<AggregatedEvent>
    ) {
        val state: ValueState<AggregatedEvent> = context.globalState().getState(stateDescriptor)

        var accumulatedEvent = state.value() ?: AggregatedEvent(userId = userId)

        for (event in events) {
            for (decayInterval in DECAY_INTERVALS) {
                var currentCount = accumulatedEvent.eventCounts.getOrPut(decayInterval) { 
                    DecayCounter(decayInterval=decayInterval, lastUpdate=0, value=0.0)
                }
                val nextEvent = DecayCounter(decayInterval = decayInterval, lastUpdate = event.eventTs, value = event.eventCount.toDouble())
                println("nextEvent: ${nextEvent}")
                currentCount += nextEvent
                println("currentCount: ${currentCount}")
                accumulatedEvent.eventCounts.set(decayInterval, currentCount)
            }
        }

        println("accumulatedEvent: ${accumulatedEvent}")
        state.update(accumulatedEvent)
        accumulatedEvent.let { collector.collect(it) }
    }
}

/** Example DynamoDB element converter. */
class CustomElementConverter : ElementConverter<AggregatedEvent, DynamoDbWriteRequest> {

    override fun apply(event: AggregatedEvent, context: SinkWriter.Context): DynamoDbWriteRequest {
        val countsMap: Map<String, AttributeValue> = event.eventCounts.mapKeys { entry ->
            entry.key.toString()
        }.mapValues { (_, decayCounter) ->
            AttributeValue.builder().m(
                mapOf(
                    "decayInterval" to AttributeValue.builder().n(decayCounter.decayInterval.toString()).build(),
                    "lastUpdate" to AttributeValue.builder().n(decayCounter.lastUpdate.toString()).build(),
                    "value" to AttributeValue.builder().n(decayCounter.value.toString()).build()
                )
            ).build()
        }

        val item = hashMapOf<String, AttributeValue>(
            "userId" to AttributeValue.builder().s(event.userId).build(),
            "counts" to AttributeValue.builder().m(countsMap).build()
        )

        return DynamoDbWriteRequest.builder()
            .setType(DynamoDbWriteRequestType.PUT)
            .setItem(item)
            .build()
    }
}