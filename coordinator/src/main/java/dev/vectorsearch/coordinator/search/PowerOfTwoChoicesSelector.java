package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.common.ring.Partition;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Power of two choices: sample two candidates at random and send to the one with the lower
 * {@link NodeStats#cost} (latency x in-flight). With replication factor 2 the "sample" is always both holders,
 * so this becomes "the better of the two"; the random part only breaks ties.
 */
public final class PowerOfTwoChoicesSelector implements ReplicaSelector {
    private final NodeStats stats;

    public PowerOfTwoChoicesSelector(NodeStats stats) {
        this.stats = stats;
    }

    @Override
    public String choose(Partition partition, List<String> candidates) {
        if (candidates.size() == 1) return candidates.get(0);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int i = rnd.nextInt(candidates.size());
        int j = rnd.nextInt(candidates.size() - 1);
        if (j >= i) j++; // two distinct indexes
        String a = candidates.get(i), b = candidates.get(j);
        double ca = stats.cost(a), cb = stats.cost(b);
        if (ca == cb) return rnd.nextBoolean() ? a : b;
        return ca < cb ? a : b;
    }
}
