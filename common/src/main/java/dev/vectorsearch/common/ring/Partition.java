package dev.vectorsearch.common.ring;

import java.util.List;

/**
 * A slice of the data stored together in one HNSW index on each of its holders.
 *
 * <p>Partition = the replica set {primary, replica} of the ring arcs it covers (unordered, so the arcs
 * where node-1 is primary and node-2 replica and the arcs where node-2 is primary and node-1 replica are
 * one partition "node-1.node-2"). Every partition therefore lives on exactly two nodes with identical
 * contents, which is what replica selection, hedging and failover need. See step 22.
 *
 * @param id      holders sorted and joined with '.', e.g. {@code node-1.node-3}; valid as a file name
 * @param holders sorted node ids (one with a single-node ring, otherwise two)
 */
public record Partition(String id, List<String> holders) {
    public static final char SEPARATOR = '.';

    public Partition {
        holders = List.copyOf(holders);
    }

    public static Partition of(String a, String b) {
        if (b == null || a.equals(b)) return new Partition(a, List.of(a));
        return a.compareTo(b) < 0 ? new Partition(a + SEPARATOR + b, List.of(a, b)) : new Partition(b + SEPARATOR + a, List.of(b, a));
    }

    public boolean heldBy(String node) {
        return holders.contains(node);
    }

    /** The holder that is not {@code node} (or {@code node} itself on a single-node ring). */
    public String other(String node) {
        for (String h : holders) if (!h.equals(node)) return h;
        return node;
    }
}
