package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.ring.Partition;
import io.grpc.Deadline;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Step 33: round robin vs power-of-two-choices when one node is slow, and balance when none is. */
class ReplicaSelectionIT {
    @TempDir
    Path dir;
    ClusterFixture c;

    @BeforeEach
    void up() throws Exception {
        c = new ClusterFixture(dir, 1500);
    }

    @AfterEach
    void down() throws Exception {
        c.close();
    }

    record Run(double meanMs, double p99Ms, double slowShare) {}

    /** Sequential queries; slowShare = fraction of requests for node-1's partitions that node-1 served. */
    Run run(ReplicaSelector selector, NodeStats stats, int queries) {
        ScatterGather sg = new ScatterGather(c.view, c.channels, selector, stats, 0, Duration.ofMillis(5), Duration.ofMillis(500));
        c.nodes.values().forEach(n -> n.searchesPerPartition.clear());
        Random rnd = new Random(11);
        long[] ns = new long[queries];
        for (int i = 0; i < queries; i++) {
            long t0 = System.nanoTime();
            sg.search(ClusterFixture.unit(rnd), 10, Deadline.after(2, TimeUnit.SECONDS));
            ns[i] = System.nanoTime() - t0;
        }
        Arrays.sort(ns);
        int slow = c.nodes.get("node-1").searches();
        int total = 0;
        for (Partition p : c.ring.partitionsOf("node-1")) {
            for (String h : p.holders()) total += c.nodes.get(h).searchesPerPartition.getOrDefault(p.id(), new AtomicInteger()).get();
        }
        return new Run(Arrays.stream(ns).average().orElseThrow() / 1e6, ns[(int) (queries * 0.99)] / 1e6, slow / (double) total);
    }

    @Test
    void p2cRoutesAroundASlowReplicaRoundRobinDoesNot() {
        run(new RoundRobinSelector(), new NodeStats(), 50); // warm up JIT and connections
        c.nodes.get("node-1").searchDelayMillis = 15;
        Run rr = run(new RoundRobinSelector(), new NodeStats(), 200);
        NodeStats stats = new NodeStats(); // the selector reads the same stats ScatterGather records into
        Run p2c = run(new PowerOfTwoChoicesSelector(stats), stats, 200);
        System.out.printf("node-1 +15 ms, 200 sequential queries:%n  round-robin mean=%.2fms p99=%.2fms node-1 served %.0f%% of its partitions' requests%n"
                + "  p2c         mean=%.2fms p99=%.2fms node-1 served %.0f%%%n", rr.meanMs, rr.p99Ms, rr.slowShare * 100, p2c.meanMs, p2c.p99Ms, p2c.slowShare * 100);
        assertTrue(rr.slowShare > 0.4 && rr.slowShare < 0.6, "round robin ignores latency: " + rr.slowShare);
        assertTrue(p2c.slowShare < 0.15, "p2c mostly avoids the slow node, only re-probing it: " + p2c.slowShare);
        assertTrue(p2c.meanMs < rr.meanMs / 2, "mean " + p2c.meanMs + " vs " + rr.meanMs);
    }

    /**
     * Equal nodes. With one query at a time nothing is ever in flight, so p2c simply sticks to whichever holder
     * measured faster (pure noise between equal nodes); that is harmless because there is no load to balance.
     * Under concurrent load the in-flight factor kicks in and spreads requests over both holders.
     */
    @Test
    void p2cSpreadsLoadOverEqualNodesUnderConcurrency() throws Exception {
        NodeStats stats = new NodeStats();
        ScatterGather sg = new ScatterGather(c.view, c.channels, new PowerOfTwoChoicesSelector(stats), stats, 0, Duration.ofMillis(5), Duration.ofMillis(500));
        c.nodes.values().forEach(n -> n.searchDelayMillis = 2); // a little service time so requests overlap
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            int seed = t;
            threads[t] = Thread.ofPlatform().start(() -> {
                Random rnd = new Random(seed);
                for (int i = 0; i < 100; i++) sg.search(ClusterFixture.unit(rnd), 10, Deadline.after(2, TimeUnit.SECONDS));
            });
        }
        for (Thread t : threads) t.join();
        for (Partition p : c.ring.partitions()) {
            int a = c.nodes.get(p.holders().get(0)).searchesPerPartition.getOrDefault(p.id(), new AtomicInteger()).get();
            int b = c.nodes.get(p.holders().get(1)).searchesPerPartition.getOrDefault(p.id(), new AtomicInteger()).get();
            System.out.printf("equal nodes, 8 concurrent clients, p2c: %s %s=%d %s=%d%n", p.id(), p.holders().get(0), a, p.holders().get(1), b);
            assertTrue(Math.min(a, b) >= 0.2 * (a + b), p.id() + " " + a + "/" + b + ": in-flight spreads the load");
        }
    }
}
