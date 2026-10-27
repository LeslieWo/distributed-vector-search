package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.common.ring.Partition;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Alternates between the holders: blind to load and latency, but spreads requests evenly. */
public final class RoundRobinSelector implements ReplicaSelector {
    private final AtomicLong next = new AtomicLong();

    @Override
    public String choose(Partition partition, List<String> candidates) {
        return candidates.get((int) Math.floorMod(next.getAndIncrement(), (long) candidates.size()));
    }
}
