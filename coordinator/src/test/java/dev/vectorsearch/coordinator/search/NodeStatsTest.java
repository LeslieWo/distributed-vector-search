package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class NodeStatsTest {
    final AtomicLong now = new AtomicLong();
    final NodeStats stats = new NodeStats(1000, now::get); // tau = 1 s

    static final long MS = 1_000_000;

    @Test
    void aSlowSampleCountsAtOnceAFastOneIsBlendedInOverTime() {
        stats.started("a");
        stats.finished("a", 50 * MS);
        assertEquals(50 * MS, stats.latencyNanos("a"), 1);
        now.addAndGet(10 * MS); // 10 ms later a fast answer: weight exp(-10/1000) keeps most of the old value
        stats.started("a");
        stats.finished("a", MS);
        double l = stats.latencyNanos("a");
        assertTrue(l > 40 * MS && l < 50 * MS, "blended slowly: " + l / MS);
        now.addAndGet(10 * MS);
        stats.started("a");
        stats.finished("a", 80 * MS);
        assertEquals(80 * MS, stats.latencyNanos("a"), 1); // peak
    }

    @Test
    void withoutNewSamplesTheEstimateDecaysSoASlowNodeGetsRetried() {
        stats.started("slow");
        stats.finished("slow", 100 * MS);
        now.addAndGet(3000 * MS); // 3 tau
        assertTrue(stats.latencyNanos("slow") < 6 * MS, "after 3 s: " + stats.latencyNanos("slow") / MS + " ms");
    }

    @Test
    void costGrowsWithRequestsInFlight() {
        stats.started("a");
        stats.finished("a", 2 * MS);
        stats.started("b");
        stats.finished("b", 2 * MS);
        stats.started("a");
        stats.started("a");
        assertEquals(2, stats.inFlight("a"));
        assertTrue(stats.cost("a") > 2.9 * stats.cost("b"), "3x: two in flight plus the new one");
        assertEquals("b", new PowerOfTwoChoicesSelector(stats).choose(null, java.util.List.of("a", "b")));
    }
}
