package dev.vectorsearch.common.cluster;

import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.common.ring.Partition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One consistent picture of the cluster, as seen by a writer or reader at some moment.
 *
 * <p>Placement and liveness are deliberately separate:
 * <ul>
 *   <li>{@code ring} decides where data lives. It changes only when an operator adds/removes a node
 *       (step 27), never because a node missed a heartbeat; otherwise every crash would reshuffle data.
 *   <li>{@code pending} is the next ring while a rebalance is copying data (null otherwise). Writers
 *       write to the owners under both rings so nothing written during the copy is missed.
 *   <li>{@code live} maps node id -> "host:grpcPort" for nodes whose heartbeat is current.
 * </ul>
 */
public record ClusterView(long ringVersion, HashRing ring, long pendingVersion, HashRing pending, Map<String, String> live) {
    public ClusterView {
        live = Map.copyOf(live);
        if (pending == null) pendingVersion = -1;
    }

    /** View without a rebalance in progress. */
    public ClusterView(long ringVersion, HashRing ring, Map<String, String> live) {
        this(ringVersion, ring, -1, null, live);
    }

    /** False until an operator initialised the ring (ClusterAdmin init). */
    public boolean hasRing() {
        return ring != null;
    }

    public boolean isLive(String node) {
        return live.containsKey(node);
    }

    public String address(String node) {
        return live.get(node);
    }

    /** A copy of a chunk to write: on {@code node}, inside partition {@code partition}. */
    public record Target(String node, String partition) {}

    /** Holders of the chunk's partition under the current ring, plus under the pending ring if one exists. */
    public List<Target> writeTargets(String chunkId) {
        List<Target> out = new ArrayList<>(4);
        add(out, ring.partitionOf(chunkId));
        if (pending != null) add(out, pending.partitionOf(chunkId));
        return out;
    }

    private static void add(List<Target> out, Partition p) {
        for (String n : p.holders()) {
            Target t = new Target(n, p.id());
            if (!out.contains(t)) out.add(t);
        }
    }
}
