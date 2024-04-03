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


import java.util.TreeMap

val TOP_SIZE = 500
val FEATURE_NAME = "view"
val DECAY_INTERVAL: Long = 15

data class Candidate(
    val mediaId: String,
    var score: Double
)

data class CandidateList(
    val candidates: List<Candidate>,
)

/*
    Maintains TOP_SIZE candidates with the highest score
 */
class TopManager {
    val scoresById = mutableMapOf<String, Double>()
    // we use tree map score -> set of ids, because different snaps can have the same score
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
        while (idsByScore.size > TOP_SIZE) {
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

        return topCandidates
    }
}

class CGMapFn : RichFlatMapFunction<AggregatedEvent, CandidateList>() {
    // non-persistent, in-memory state
    @Transient
    private var topManager = TopManager()

    override fun flatMap(event: AggregatedEvent, out: Collector<CandidateList>) {
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
            out.collect(CandidateList(listOf(candidate)))
        }
    }
}

// [ ] TODO! write code here
// combine-style intermediate aggregation
class CGSumFn : ReduceFunction<CandidateList> {
    override fun reduce(value1: CandidateList, value2: CandidateList): CandidateList {
        val topManager = TopManager()
        value1.candidates.forEach {candidate ->
            topManager.consumeCandidate(candidate)
        }
        value2.candidates.forEach {candidate ->
            topManager.consumeCandidate(candidate)
        }
        return CandidateList(topManager.getTop())
    }
}

class CGProcessEventsFn :
    ProcessAllWindowFunction<CandidateList, CandidateList, TimeWindow>() {
    
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
        candidateLists: Iterable<CandidateList>,
        collector: Collector<CandidateList>
    ) {
        val state: ListState<Candidate> = context.globalState().getListState(stateDescriptor)

        val topManager = TopManager()
        state.get().forEach {candidate ->
            topManager.consumeCandidate(candidate)
        }
        candidateLists.forEach {candidates ->
            candidates.candidates.forEach {candidate ->
                topManager.consumeCandidate(candidate)
            }
        }
        val result = topManager.getTop()
        state.update(result)
        collector.collect(CandidateList(result))
    }
}

/** DynamoDB element converter. */
class CGElementConverter : ElementConverter<CandidateList, DynamoDbWriteRequest> {

    override fun apply(candidates: CandidateList, context: SinkWriter.Context): DynamoDbWriteRequest {
        // Convert each Candidate into a Map of AttributeValue
        val candidatesAttributeValue: Map<String, AttributeValue> = candidates.candidates.map { candidate ->
            mapOf(
                "mediaId" to AttributeValue.builder().s(candidate.mediaId).build(),
                "score" to AttributeValue.builder().n(candidate.score.toString()).build()
            )
        }.mapIndexed { index, candidateMap ->
            // Use index as a key to ensure unique keys for each candidate in the map
            index.toString() to AttributeValue.builder().m(candidateMap).build()
        }.toMap()

        // Wrap the candidates in another map under the key "Candidates"
        val item = hashMapOf<String, AttributeValue>(
            "CGName" to AttributeValue.builder().s("TopViews").build(),
            "Candidates" to AttributeValue.builder().m(candidatesAttributeValue).build()
        )
        println("Writing ${item} to dynamodb")
        // Create and return the DynamoDbWriteRequest
        return DynamoDbWriteRequest.builder()
            .setType(DynamoDbWriteRequestType.PUT)
            .setItem(item)
            .build()
    }
}