package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.proto.v1.ScoredChunk;
import io.grpc.Deadline;
import java.util.List;

/** Searches every partition of the cluster for one query vector and merges the results. */
public interface ShardSearcher {

    /**
     * @param hits              global top-k, best first
     * @param missingPartitions partitions that could not be searched (both replicas failed or out of time)
     */
    record Result(List<ScoredChunk> hits, List<String> missingPartitions) {}

    Result search(float[] query, int k, Deadline deadline);
}
