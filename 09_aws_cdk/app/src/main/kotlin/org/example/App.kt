@file:JvmName("WordCount")

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.connector.source.Source
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time as WindowTime
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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.InputStream

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

data class SinkConfig(
    val awsEndpoint: String?,
    val awsRegion: String?,
    val tableName: String,
)

data class AppConfig(
    val source: SourceConfig,
    val sink: SinkConfig,
)

fun loadConfig(): AppConfig {
    val yamlReader = ObjectMapper(YAMLFactory()).registerKotlinModule()
    val inputStream: InputStream = Thread.currentThread().contextClassLoader.getResourceAsStream("config.yaml")
        ?: throw IllegalArgumentException("file not found!")
    return yamlReader.readValue(inputStream, AppConfig::class.java)
}

val DECAY_INTERVALS = longArrayOf(5, 10, 15)
val ALLOWED_EVENTS = arrayOf("view", "like")

@Serializable
data class Event(
    val eventName: String = "",
    val userId: String = "",
    val creatorId: String = "",
    val eventTs: Long = 0,
    var eventCount: Long = 0,
)

data class AggregatedEvent(
    val featureName: String = "",
    val key: String = "",
    var eventCounts: HashMap<Long, DecayCounter> = hashMapOf()
)

data class KeyPair(
    val eventName: String,
    val key: String,
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

fun initKinesisSource(config: SourceConfig): FlinkKinesisConsumer<String> {
    val properties = Properties()
    if (config.awsEndpoint != null) {
        properties[AWSConfigConstants.AWS_ENDPOINT] = config.awsEndpoint
    }
    if (config.awsRegion != null) {
        properties[AWSConfigConstants.AWS_REGION] = config.awsRegion
    }
    if (config.awsAccessKeyId != null) {
        properties[AWSConfigConstants.AWS_ACCESS_KEY_ID] = config.awsAccessKeyId
    }
    if (config.awsSecretAccessKey != null) {
        properties[AWSConfigConstants.AWS_SECRET_ACCESS_KEY] = config.awsSecretAccessKey
    }
    if (config.awsAssumeRoleArn != null) {
        properties[AWSConfigConstants.AWS_CREDENTIALS_PROVIDER] = "ASSUME_ROLE"
        properties[AWSConfigConstants.AWS_ROLE_ARN] = config.awsAssumeRoleArn
        properties[AWSConfigConstants.AWS_ROLE_SESSION_NAME] = "ksassumedrolesession"
    }
    properties[ConsumerConfigConstants.STREAM_INITIAL_POSITION] = config.streamInitialPosition
    val kinesisConsumer = FlinkKinesisConsumer(config.datastreamName, SimpleStringSchema(), properties)
    return kinesisConsumer
}

fun initDynamodbSink(config: SinkConfig): DynamoDbSink<AggregatedEvent> {
    val properties = Properties()
    if (config.awsEndpoint != null) {
        properties[AWSConfigConstants.AWS_ENDPOINT] = config.awsEndpoint
    }
    if (config.awsRegion != null) {
        properties[AWSConfigConstants.AWS_REGION] = config.awsRegion
    }

    val dynamoDbSink = DynamoDbSink.builder<AggregatedEvent>()
        .setTableName(config.tableName)
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("key"))
        .setDynamoDbProperties(properties)
        .build()
    return dynamoDbSink
}

fun runJob() {
    val config = loadConfig()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val kinesisConsumer = initKinesisSource(config.source)
    val kinesisSource = env.addSource(kinesisConsumer)
    // checkpoint every minute
    // env.enableCheckpointing(60000);
    val dynamoDbSink = initDynamodbSink(config.sink)

    defineWorkflow(kinesisSource, config) { workflow -> 
            workflow.sinkTo(dynamoDbSink) 
        }
    env.execute()
}

fun defineWorkflow(
    source: DataStream<String>,
    config: AppConfig,
    sinkApplier: (stream: DataStream<AggregatedEvent>) -> Unit
) {
    var watermarkStrategy = WatermarkStrategy
        .forMonotonousTimestamps<Event>()
    if (config.source.kinesaliteTimestampFix) {
        watermarkStrategy = watermarkStrategy.withTimestampAssigner { _event: Event, timestamp: Long -> timestamp * 1000
        }
    } else {
        watermarkStrategy = watermarkStrategy.withTimestampAssigner { _event: Event, timestamp: Long -> timestamp
        }
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

class Tokenizer : FlatMapFunction<String, Event> {
    override fun flatMap(line: String, out: Collector<Event>) {
        val json = Json { ignoreUnknownKeys = true } // Create a Json instance with configuration
        val event = json.decodeFromString(Event.serializer(), line)
        if (!(event.eventName in ALLOWED_EVENTS)) {
            return
        }
        event.eventCount = 1
        println("Decoded event from json: ${event}")
        out.collect(event)
    }
}

class Sum : ReduceFunction<Event> {
    override fun reduce(value1: Event, value2: Event): Event {
        println("Reduce for ${value1} and ${value2}")
        value1.eventCount += value2.eventCount
        return value1
    }
}

class ProcessEvents :
    ProcessWindowFunction<Event, AggregatedEvent, KeyPair, TimeWindow>() {
    
    private lateinit var stateDescriptor: ValueStateDescriptor<AggregatedEvent>

    override fun open(parameters: Configuration) {
        val ttlConfig = StateTtlConfig.newBuilder(Time.days(7))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        stateDescriptor = ValueStateDescriptor("AggregatedEvent", AggregatedEvent::class.java)
        stateDescriptor.enableTimeToLive(ttlConfig)
        super.open(parameters)
    }

    override fun process(
        keyPair: KeyPair,
        context: Context,
        events: Iterable<Event>,
        collector: Collector<AggregatedEvent>
    ) {
        val state: ValueState<AggregatedEvent> = context.globalState().getState(stateDescriptor)

        var accumulatedEvent = state.value() ?: AggregatedEvent(featureName = keyPair.eventName, key = keyPair.key)
        println("ProcessEvents started!")
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
        println("CustomElementsConverter for ${event}")
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
            "key" to AttributeValue.builder().s(event.key).build(),
            "featureName" to AttributeValue.builder().s(event.featureName).build(),
            "counts" to AttributeValue.builder().m(countsMap).build()
        )

        return DynamoDbWriteRequest.builder()
            .setType(DynamoDbWriteRequestType.PUT)
            .setItem(item)
            .build()
    }
}