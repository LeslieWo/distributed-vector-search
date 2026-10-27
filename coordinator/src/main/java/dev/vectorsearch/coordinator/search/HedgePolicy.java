package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.common.ring.Partition;

/** How long to wait for a partition's first replica before also asking the other one. */
public interface HedgePolicy {
    long delayNanos(Partition partition);

    /** Latency of one successful attempt against this partition (feeds adaptive policies). */
    default void record(Partition partition, long latencyNanos) {}
}
