Setup instructions: [link](setup.md)

In the previous tutorial, we've built a "counter machine", that can be used to generate all sorts of counter-based features to be used in our recommender system.
When using these features, at request time, the web service responsible for feed generation will have to fetch features for the user and a list of posts.

What if the overall corpus size (posts available for recommendations) has millions of elements? We will have to request features for a million keys at a time. This is prohibitively slow and expensive.   

To avoid that, one typical solution is to use "candidate generators". They aim to provide an initial, filtered set of posts to be used in heavier models later on.   

Few examples of candidate generators (cgs):
- Knn-based cg, selecting posts with embedding closest to that of the user
- Counter-based cgs, selecting top liked posts over the last week
- Min views cg, selecting random posts with less then 100 views
- Creator Affinity cg, selecting posts from creators the user followed or interacted with before.   


In this tutorial, we will focus on "Counter Based" cgs, and show how to build them in Flink, reusing information already generated in the counter machine.    

Using flink for candidate generation is a good way to make sure candidates in the CG are updated real-time, which is as important as updating the features. Alternatives, like a batch process that selects top candidates once a day, will always have "staleness" problem:
- New posts will take a lot of time to make it into the primary cg
- Posts that just became trending will not be picked up by the cg for some time
- Posts that "died" and stopped being trending will keep showing as candidates for some time   

## The logic
Let's start with a simple logic for candidate generation: `select top 500 posts with the most likes over the last day`. Similar techniques can be used to code down many logic examples, like "top fresh posts by likes in the last 15 minutes" as well as "top posts by overall likes", though some advanced logic like "top posts by like to view ratio amongst posts with more than 100 views" are a bit harder and will require streaming joins.  

In our counter machine code, we already calculate counters like "total number of likes over the last day". So we just need to select a specified counter for each post, then aggregate all the posts into global top-500.   

First, let's write logic for score aggregation. This is pure kotlin, so we can write it and test locally:  
```kotlin
val TOP_SIZE = 500

@Serializable
data class Candidate(
    val mediaId: String,
    var score: Double
): java.io.Serializable

/*
    Maintains TOP_SIZE candidates with the highest score
 */
@Serializable
class TopManager: java.io.Serializable {
    val scoresById = mutableMapOf<String, Double>()
    // we use tree map score -> set of ids, because different snaps can have the same score
    @Transient
    val idsByScore = TreeMap<Double, MutableSet<String>>()

    fun consumeCandidate(candidate: Candidate): Boolean {
        val oldScore = scoresById[candidate.mediaId]
        val newScore = candidate.score
        val id = candidate.mediaId
        
        // If the ID is already in the map, remove it (might insert back later)
        if (oldScore != null) {
            val ids = idsByScore[oldScore]
            ids?.remove(id)
            if (ids.isNullOrEmpty()) {
                idsByScore.remove(oldScore)
            }
            scoresById.remove(id)
        }

        scoresById[id] = newScore
        // attempt to add the new score
        idsByScore.getOrPut(newScore) { mutableSetOf() }.add(id)

        // Ensure we only keep the top TOP_SIZE
        // potentially removes just added score
        while (scoresById.size > TOP_SIZE) {
            val firstEntry = idsByScore.firstEntry()
            val idsToRemove = firstEntry.value
            if (!idsToRemove.isNullOrEmpty()) {
                val idToRemove = idsToRemove.first()
                idsToRemove.remove(idToRemove)
                scoresById.remove(idToRemove)
                if (idsToRemove.isEmpty()) {
                    idsByScore.pollFirstEntry()
                }
            }
        }

        // indicates whether new candidate was inserted or not
        if (scoresById[candidate.mediaId] != null && (oldScore == null || newScore > oldScore)) {
            return true
        } else {
            return false
        }
    }

    fun getTop(): List<Candidate> {
        val topCandidates = mutableListOf<Candidate>()
        idsByScore.descendingMap().forEach { (score, ids) ->
            ids.forEach { id ->
                topCandidates.add(Candidate(id, score))
            }
        }
        println("TopManager.getTop, current size: ${topCandidates.size}")
        return topCandidates
    }

    private fun initializeIdsByScore() {
        scoresById.forEach { (id, score) ->
            idsByScore.getOrPut(score) { mutableSetOf() }.add(id)
        }
    }

    init {
        initializeIdsByScore()
    }
}
```
The `TopManager` class is responsible for keeping track of current top 500 posts by score, whatever the definition for the score is. It uses TreeMap to keep track of current top-500 post ids; we need to use the score as key for TreeMap's sorting; multiple posts can have exactly the same score, so we need to have a set of ids as value.   
## ProcessAllWindowFunction
When aggregating counters in the counter machine, we used "reduce" operation on a so-called "Keyed" stream - stream grouped by a certain key, like postId or userId. Now, we don't really have a "key" to speak of, we need some sort of "global" reduce. There are 2 ways to achieve this: either use a bogus "key" with constant value, like `""`, or use a so-called `windowAll` operation. We'll go for the latter. First, we define a ProcessWindowAll function: it takes in all events that happened within a specified time window (5 secs) globally, and produces single output.  
```kotlin
class CGProcessEventsFn :
    ProcessAllWindowFunction<TopManager, TopManager, TimeWindow>() {
    
    private lateinit var stateDescriptor: ListStateDescriptor<Candidate>

    override fun open(parameters: Configuration) {
        val ttlConfig = StateTtlConfig.newBuilder(Time.days(7))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        stateDescriptor = ListStateDescriptor("CGProcessEventsFn", Candidate::class.java)
        stateDescriptor.enableTimeToLive(ttlConfig)
        super.open(parameters)
    }

    override fun process(
        context: Context,
        candidateLists: Iterable<TopManager>,
        collector: Collector<TopManager>
    ) {
        val state: ListState<Candidate> = context.globalState().getListState(stateDescriptor)

        val topManager = TopManager()
        state.get().forEach {candidate ->
            topManager.consumeCandidate(candidate)
        }
        candidateLists.forEach {candidates ->
            candidates.getTop().forEach {candidate ->
                topManager.consumeCandidate(candidate)
            }
        }
        state.update(topManager.getTop())
        collector.collect(topManager)
    }
}
```
Note the slight difference to `ProcessWindowFunction` we had before: it is now `ProcessAllWindowFunction`, and it doesn't have a template argument for key type, because there is no key to speak of. Remainder of the logic is more or less the same: we keep persistent state to prevent us from losing all candidate data in case of worker outages, we iterate over all inputs for the specified time window, and produce a single output - which is itself a list of up to 500 posts.  
## Reduce Operator for some optimizations
The function above has one vulnerability: it will always be executed on one node, because there is no partitioning logic that can be applied. If we have trillions of operations per second coming in, the node will simply not be able to handle all the load. In addition, with 5-second window aggregation, Flink will have to keep all the events for current window in memory before even starting to process it, so it will hold around 5 trillion events in memory on average.  

Our counter machine reducer mitigates that partly: it reduces all events by postId, generating only 1 record per postId per 5 seconds. However, if there are lots of unique posts, that will not help completely.   

Another optimization we can make is to provide a binary "reduce" operator for intermediate aggregation: 
```kotlin

// combine-style intermediate aggregation
class CGSumFn : ReduceFunction<TopManager> {
    override fun reduce(value1: TopManager, value2: TopManager): TopManager {
        // choose smaller value for combine
        if (value1.scoresById.size > value2.scoresById.size) {
            value2.getTop().forEach {candidate ->
                value1.consumeCandidate(candidate)
            }
            return value1
        } else {
            value1.getTop().forEach {candidate ->
                value2.consumeCandidate(candidate)
            }
        }
        return value2
    }
}
```
Its job is to do intermediate aggregation while the window is still not closed; new events coming in will be passed through this reduce function. So instead of keeping 5 trillion of events in memory, it will be reduced down to a single record with up to 500 posts.   

We also want to keep complexity of this code minimal; flink gives no guarantees about the order in which this reduce operator will be applied. It can either take current aggregated state (big state, 500 posts), and add a new incoming events to it; or take a huge list of events and combine them in a divide-and-conquer style. To be ready for all cases, we always check which value has the smaller number of posts and optimize accordingly.
## Map function and some more optimizations
So far, all our reduce and process functions operate on incoming `TopManager` events. But we need to get those first; it can be done in a map operation. The same operation will have the logic of selecting which exact counter and feature name to use as a score.
```kotlin
class CGMapFn : RichFlatMapFunction<AggregatedEvent, TopManager>() {
    // non-persistent, in-memory state
    @Transient
    private var topManager = TopManager()

    override fun flatMap(event: AggregatedEvent, out: Collector<TopManager>) {
        if (topManager == null) {
            topManager = TopManager()
        }
        if (event.featureName != FEATURE_NAME) {
            return
        }
        val score: Double? = event.eventCounts[DECAY_INTERVAL]?.value
        if (score == null) {
            return
        }
        val candidate = Candidate(event.key, score)
        val isInserted = topManager.consumeCandidate(candidate)
        // output some data only if the snap actually made it into the top
        if (isInserted) {
            val result = TopManager()
            result.consumeCandidate(candidate)
            out.collect(result)
        }
    }
}
```
This map operation triggers on every incoming AggregatedEvent (1 event per unique postId per 5 seconds for this partition). As a load-limiting measure, we also perform on-the-fly top aggregation. We keep our own TopManager in each mapper; it is marked as Transient to signify that we don't actually care about it's state persistence.   

It keeps its own top-500 posts, and only sends output events when a new post makes it into the top, or when the score is updated upwards for the post that is already in the top. This should in theory filter out majority of events that should not affect the final top.
## Final workflow
```kotlin
fun defineWorkflow(
    source: DataStream<String>,
    config: AppConfig,
) {
    // defining sinks
    val properties = Properties()
    if (config.sink.awsEndpoint != null) {
        properties[AWSConfigConstants.AWS_ENDPOINT] = config.sink.awsEndpoint
    }
    if (config.sink.awsRegion != null) {
        properties[AWSConfigConstants.AWS_REGION] = config.sink.awsRegion
    }

    val countersSink = DynamoDbSink.builder<AggregatedEvent>()
        .setTableName(config.sink.tableName)
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("key"))
        .setDynamoDbProperties(properties)
        .build()

    val cgSink = DynamoDbSink.builder<TopManager>()
        .setTableName(config.sink.cgTableName)
        .setElementConverter(CGElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("CGName"))
        .setDynamoDbProperties(properties)
        .build()


    // workflow logic
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
                println("Key by mediaId: ${value.mediaId}")
                KeyPair(eventName = value.eventName, key = value.mediaId) 
            }
        .window(TumblingEventTimeWindows.of(WindowTime.seconds(5)))
        .reduce(Sum(), ProcessEvents())
        .name("counter")

    counts.sinkTo(countersSink)

    // candidate generation
    val cgResult = counts.flatMap(CGMapFn())
        .name("cgMap")
        .windowAll(TumblingProcessingTimeWindows.of(WindowTime.seconds(5)))
        .reduce(CGSumFn(), CGProcessEventsFn())
        .name("cgReduce")
    
    cgResult.sinkTo(cgSink)
}
```
We take `counts` stream from previous tutorial, and apply `flatMap` -> `windowAll` -> `reduce` operations on top of it. We also define second sink, to write our candidate data.   

Since the format is different, we also need a separate dynampdb write request function: 
```kotlin
class CGElementConverter : ElementConverter<TopManager, DynamoDbWriteRequest> {

    override fun apply(candidates: TopManager, context: SinkWriter.Context): DynamoDbWriteRequest {
        // Convert each Candidate into a Map of AttributeValue, then into AttributeValue.M (a DynamoDB Map)
        val candidatesAttributeValues: List<AttributeValue> = candidates.getTop().map { candidate ->
            val candidateMap = mapOf(
                "mediaId" to AttributeValue.builder().s(candidate.mediaId).build(),
                "score" to AttributeValue.builder().n(candidate.score.toString()).build()
            )
            // Convert the map into an AttributeValue object representing a DynamoDB Map
            AttributeValue.builder().m(candidateMap).build()
        }

        // Now, instead of wrapping the candidates in a map, create an AttributeValue list (L)
        val item = hashMapOf<String, AttributeValue>(
            "CGName" to AttributeValue.builder().s("TopViews").build(),
            "Candidates" to AttributeValue.builder().l(candidatesAttributeValues).build() // This creates a list type
        )

        println("Writing ${item} to dynamodb")
        // Create and return the DynamoDbWriteRequest with the modified structure
        return DynamoDbWriteRequest.builder()
            .setType(DynamoDbWriteRequestType.PUT)
            .setItem(item)
            .build()
    }
}
```