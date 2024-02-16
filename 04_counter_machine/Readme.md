Now we are finally getting to some useful stuff.  

Imagine you are building a social network app. You have a feed with posts, and users might like the post or just scroll through it, viewing it but not liking it. You want to use ML for your feed generation, and you need to generate some features for it.   

One way to do that is to create so called "Counter Machine". Think of the following SQL query:   
```SQL
select count_if(event_name = "like")/count_if(event_name = "vliew") as ctr, post_id from `...` where timestamp > now() - 1_WEEK group by post_id
```
We can modify parameters of this query to arrive at various features:    

- time filtering: 1 minute, 1 hour, 1 day, 1 week, 1 month, …: allows capturing short term and long-term trends (does this snap has a lot of likes because it was around for a long time? Or is it “fresh” and just exploding in popularity?)
- aggregation keys
    - group by post_id: popularity feature. Shows how good current snap is. Using only popularity-based features in the ranker will mean that the feed is equal for all users
    - group by user_id: user cohort feature. Shows how active this user is, is he a returning user or a new user etc. Introduces a little bit of personalization on cohort level
    - group by user_id x post_id: sometimes called “cross features”. Can introduce some personalization. Completely useless if we do not want to show same post to the same user twice
    - group by user_id x creator_id: user to creator affinity. Main source of personalization, apart from CF-style neural networks
    - group_by user_id x category_id
- aggregations: total like count, like count divided by view count, number of unique users who liked
- engagements: likes, views, timespent, clicks, creator profile clicks, comments

Typically, during ML research, one would generate around 1000 counters by using all possible combinations of parameters above. Then perform feature selection process to filter out useless ones.   

Now let's discuss how to build something like that in Flink.   
## Decay Counters
The "timestamp" filtering from above does not really work great for streaming processing. If we want to use events from the past month in our aggregation, we will have to store all these events, which becomes costly.   

Alternative is to use "decaying counter". Let's say we had a counter value of 1.0 at a certain time point. 1 hour later, we get a counter increment of 2.0. But we want to "decay" the old value using exponential decay formula:  
$$value = value * e^{-\frac{timeDelta}{decayInterval}}$$
`decayInterval` is the constant that controls how quick counter value decays over time. With interval of 1 hour, counter value will decay by approximately 2.7 over the course of an hour, and will decay to zero over a few hours. With interval of 1 month, the counter will hardly change in an hour, but will decay over the course of a few months. In this way, we can effectively capture the same "short term" and "long term" trends, but we only need to store 2 numbers: current counter value and last update time.   

Here is the code for such a counter in Kotlin:   
```Kotlin
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
```
We have just 1 function in our interface - "+" operator. The result of applying such operator to two counters will yield a counter with update time equal to max of two update times, and value as the sum of the later counter with attenuated, exponentially decayed value of the earlier one.  

Note that this "sum" operation is similar to normal "sum" in that we can apply it to a bunch of different values step by step, and arrive at the same result irrespective of the order in which we applied it. This allows us not to worry about the precise order of events as they come.   

## Parsing input
Let's assume data comes in in the following format:  
```
{"eventName":"like","userId":"1234","creatorId":"7777","eventTs":1707605599}
```
To parse it in Kotlin, add the following dependency:  
```Kotlin
implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.4.0")
```
Define our custom Event class, and replace our previous "Tokenizer" class with:   
```Kotlin
@Serializable
data class Event(
    val eventName: String = "",
    val userId: String = "",
    val creatorId: String = "",
    val eventTs: Long = 0,
    var eventCount: Long = 0,
)

class Tokenizer : FlatMapFunction<String, Event> {
    override fun flatMap(line: String, out: Collector<Event>) {
        val json = Json { ignoreUnknownKeys = true } // Create a Json instance with configuration
        val event = json.decodeFromString(Event.serializer(), line)
        event.eventCount = 1
        println("Decoded event from json: ${event}")
        out.collect(event)
    }
}
```
Though I should've probably renamed it.   

This is the default way of parsing Json in Kotlin. We define a class with class names equal to json keys we expect to parse. `eventCount` is our custom field that is not present in the input data; we will put the actual counter value here, which is useful for future "Reduce" stage.
## Reduce operation
I decided to separate aggregation in 2 stages: reduce followed by ProcessFunction. Reduce will not care about decaying counters, because I plan to apply it over a small time window. ProcessFunction will iterate over various decay intervals and handle actual long-term state.   
```Kotlin
class Sum : ReduceFunction<Event> {
    override fun reduce(value1: Event, value2: Event): Event {
        value1.eventCount += value2.eventCount
        return value1
    }
}
```
This "Sum" reduce operator is exactly the same as we used in WordCount examples.
## ProcessFunction, aggregation over several counters
```Kotlin
val DECAY_INTERVALS = longArrayOf(5, 10, 15)

data class AggregatedEvent(
    val featureName: String = "",
    val key: String = "",
    var eventCounts: HashMap<Long, DecayCounter> = hashMapOf()
)

data class KeyPair(
    val eventName: String,
    val key: String,
)

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
        .keyBy { value -> KeyPair(eventName = value.eventName, key = value.userId) }
        .window(TumblingEventTimeWindows.of(WindowTime.seconds(5)))
        .reduce(Sum(), ProcessEvents())
        .name("counter")

    sinkApplier(counts)
}
```
Let's discuss it starting from the workflow.  

We take textLines - String objects that we got from Kafka, and apply our Tokenizer class which parses Json and outputs Event objects.  

We want to group the stream by eventName and UserId, to aggregate counters for different events. Future features like "like count divided by view count" will then be computable out of raw counter values, which is better done on the backend that will query this DynamoDB, not in Flink. Grouping by "UserId" is the same as you'd have in sql; in the future, we might want more aggregations (postId, postId + userId and so on), which we will have to add as a workflow branch.   

We apply a window with 5 second duration, and then reduce function with our "Sum" operator that operates on Event objects, followed by ProcessEvents function that takes Event as input, and outputs AggregatedEvent.   

The key used in keyBy operation will also be used for state storage in ProcessEvents function. So we don't need to worry about that. We do add state ttl however: without it, long running jobs' state will grow indefinitely.   

AggregatedEvent has eventName (renamed to featureName, because we might also have various aggregations, not just features corresponding to exact event name), key (for now all keys have userIds, but in the future we might have keys for other entities as well and we want to reuse the same code), and a map of decayCounters. Keys in the map correspond to the decayInterval, values - to the actual DecayCounter objects.   

We load a possibly missing AggregatedEvent from state, iterate over all input Event objects (though in our case there will only be 1 object because we already applied our Sum() operator to them), aggregated the decay counters (all the logic is already written in our "+" operator), then save it back to state as well as write it to the output that will be sinked into DynamoDB.   

Now, we just need to define ElementConverter for DynamoDB sink:  
```Kotlin
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
```
It stores the same map of decayCounters, along with key and featureName. We had to convert all values to String to store it in dynamodb.  

This is how we can create the table using aws-cli:   
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb create-table \
    --table-name UserCounters \
    --attribute-definitions \
        AttributeName=key,AttributeType=S \
				AttributeName=featureName,AttributeType=S \
		--key-schema \
		        AttributeName=key,KeyType=HASH \
		        AttributeName=featureName,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
		--endpoint-url http://localhost:8000
```
Note that both `key` and `featureName` have to be primary keys in the table; different events, like "like" and "view", will be processed at different times in Flink, generating independent DynamoDB put requests. This means that we can't just store different events as part of Value in the db, because then writes from one event will rewrite the data from previous event.   

You can feed [data.json](./data.json) example data to kafka to test the job. And this is how you can query dynamodb for the content of composite keys:   
```bash
AWS_DEFAULT_REGION=region AWS_ACCESS_KEY_ID=id AWS_SECRET_ACCESS_KEY=key aws dynamodb get-item --endpoint-url http://localhost:8000 --table-name UserCounters --key '{"key": {"S": "1234"}, "featureName": {"S": "like"}}'
```