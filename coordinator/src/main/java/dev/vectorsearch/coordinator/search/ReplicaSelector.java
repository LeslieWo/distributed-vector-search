package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.common.cluster.ClusterView;
import dev.vectorsearch.common.ring.Partition;
import java.util.List;

/** Picks which holder of a partition serves a query (both hold the same data, step 26). */
public interface ReplicaSelector {

    /** @param candidates live holders of the partition, at least one */
    String choose(Partition partition, List<String> candidates);

    /** Live holders of {@code p} in {@code view}, in the partition's (sorted) order. */
    static List<String> liveHolders(Partition p, ClusterView view) {
        return p.holders().stream().filter(view::isLive).toList();
    }
}
