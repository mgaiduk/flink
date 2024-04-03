import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Collector;

import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.api.connector.sink2.SinkWriter
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;

import kotlinx.serialization.*
import kotlinx.serialization.json.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.util.TreeMap

val TOP_SIZE = 2
val FEATURE_NAME = "like"
val DECAY_INTERVAL: Long = 15

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

// combine-style intermediate aggregation
class CGSumFn : ReduceFunction<TopManager> {
    override fun reduce(value1: TopManager, value2: TopManager): TopManager {
        value2.getTop().forEach {candidate ->
            value1.consumeCandidate(candidate)
        }
        return value1
    }
}

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

/** DynamoDB element converter. */
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