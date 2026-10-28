package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.shardnode.testing.TestShardNode;
import io.grpc.Deadline;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Step 34: hedged requests cut the tail, the budget caps the extra load, the loser is cancelled. */
class HedgingIT {
    static final long MS = 1_000_000;
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

    ScatterGather sg(ReplicaSelector selector, HedgePolicy policy, double budget) {
        return sg(new NodeStats(), selector, policy, budget);
    }

    ScatterGather sg(NodeStats stats, ReplicaSelector selector, HedgePolicy policy, double budget) {
        if (selector == null) selector = new PowerOfTwoChoicesSelector(stats);
        return new ScatterGather(c.view, c.channels, selector, stats, new NodeHealth(), 0, Duration.ofMillis(5), Duration.ofMillis(500),
                policy, policy == null ? null : new HedgeBudget(budget, 10), new SimpleMeterRegistry());
    }

    record Run(double p50, double p99, double max, long hedges, long won, long requests, int missing) {
        @Override
        public String toString() {
            return String.format("p50=%.2fms p99=%.2fms max=%.2fms hedges=%d won=%d (%.1f%% extra of %d partition requests) missing=%d",
                    p50, p99, max, hedges, won, 100.0 * hedges / requests, requests, missing);
        }
    }

    Run run(ScatterGather sg, int queries, long seed) {
        Random rnd = new Random(seed);
        long[] ns = new long[queries];
        int missing = 0;
        double sent0 = sg.hedgesSent.count(), won0 = sg.hedgesWon.count(), req0 = sg.partitionRequests.count();
        for (int i = 0; i < queries; i++) {
            long t0 = System.nanoTime();
            missing += sg.search(ClusterFixture.unit(rnd), 10, Deadline.after(2, TimeUnit.SECONDS)).missingPartitions().size();
            ns[i] = System.nanoTime() - t0;
        }
        Arrays.sort(ns);
        return new Run(ns[queries / 2] / 1e6, ns[(int) (queries * 0.99)] / 1e6, ns[queries - 1] / 1e6,
                (long) (sg.hedgesSent.count() - sent0), (long) (sg.hedgesWon.count() - won0), (long) (sg.partitionRequests.count() - req0), missing);
    }

    @Test
    void hedgingCutsTheTailCausedByRandomHiccups() {
        // every node: 2% of searches stall 40 ms (GC pause, noisy neighbour). Not node-specific: p2c cannot avoid it.
        // (With 5% the p95 itself lands on the stalls, the hedge delay becomes ~40 ms and hedging stops helping:
        // measured p99 45.8 -> 42.1 ms. See BUILDLOG step 34.)
        for (TestShardNode n : c.nodes.values()) n.randomDelayMillis = () -> ThreadLocalRandom.current().nextInt(100) < 2 ? 40 : 0;
        ScatterGather off = sg(null, null, 0);
        run(off, 100, 1); // warm-up
        Run noHedge = run(off, 400, 2);
        ScatterGather on = sg(null, new AdaptiveHedgePolicy(3 * MS, 10 * MS), 0.1);
        run(on, 100, 1); // warm-up, fills the p95 windows
        Run hedged = run(on, 400, 2);
        System.out.println("2% of searches +40 ms, 400 sequential queries\n  hedging off: " + noHedge + "\n  hedging on:  " + hedged);
        assertTrue(noHedge.p99 > 35, "without hedging the hiccups show up in p99: " + noHedge.p99);
        assertTrue(hedged.p99 < noHedge.p99 / 2, "hedging cuts p99: " + hedged.p99 + " vs " + noHedge.p99);
        assertTrue(hedged.hedges <= 0.1 * hedged.requests + 10, "within budget");
        assertEquals(0, hedged.missing);
    }

    @Test
    void theBudgetCapsHedgesWhenEverythingIsSlow() {
        // overload look-alike: every search takes 5 ms and the policy would hedge after 1 ms, i.e. always
        for (TestShardNode n : c.nodes.values()) n.searchDelayMillis = 5;
        ScatterGather sg = sg(null, p -> MS, 0.1);
        Run r = run(sg, 200, 3);
        System.out.println("all searches 5 ms, hedge after 1 ms, budget 10%: " + r);
        assertTrue(r.hedges <= 0.1 * r.requests + 10, "budget: " + r.hedges + " of " + r.requests);
        assertTrue(r.hedges > 0.05 * r.requests, "and it does hedge up to the budget: " + r.hedges);
    }

    @Test
    void theLosingCallIsCancelledOnTheShard() throws Exception {
        TestShardNode slow = c.nodes.get("node-1");
        slow.searchDelayMillis = 100;
        ScatterGather sg = sg(new RoundRobinSelector(), p -> 2 * MS, 1.0);
        Run r = run(sg, 60, 4);
        // node-1 has 8 threads and each call it picks up sleeps 100 ms first: wait until it has drained its queue
        Thread.sleep(1500);
        int handled = slow.searches(); // calls whose handler ran at all; gRPC drops calls cancelled while still queued
        int searched = handled - slow.skippedExpired.get(); // handler ran and the HNSW search was not skipped
        System.out.printf("node-1 +100 ms, hedge after 2 ms: %s; node-1: %d calls reached the handler, %d skipped as cancelled, %d searched%n",
                r, handled, slow.skippedExpired.get(), searched);
        assertTrue(r.won > 0);
        assertTrue(r.p99 < 50, "hedge answers long before node-1: " + r.p99);
        assertTrue(searched <= 2, "the slow node did (almost) no wasted HNSW work for calls it lost: " + searched);
    }

    /**
     * Step 41: a cancelled loser leaves NodeStats with only the hedge time (a few ms), so p2c keeps choosing a stalled
     * replica. Letting the loser finish records its real latency.
     */
    @Test
    void keepingTheLoserRecordsItsRealLatency() throws Exception {
        TestShardNode slow = c.nodes.get("node-1");
        slow.searchDelayMillis = 100;
        for (boolean cancel : new boolean[] {true, false}) {
            NodeStats stats = new NodeStats(60_000, System::nanoTime); // long tau: no decay during the test
            ScatterGather sg = sg(stats, new RoundRobinSelector(), p -> 2 * MS, 1.0);
            sg.setCancelHedgeLosers(cancel);
            run(sg, 6, 5);
            Thread.sleep(400); // let uncancelled losers answer
            double ms = stats.latencyNanos("node-1") / 1e6;
            System.out.printf("cancel losers=%s: node-1 latency estimate %.1f ms, in flight %d%n", cancel, ms, stats.inFlight("node-1"));
            if (cancel) assertTrue(ms < 50, "cancelled losers are recorded at the hedge time: " + ms);
            else assertTrue(ms >= 90, "kept losers are recorded at their real latency: " + ms);
            assertEquals(0, stats.inFlight("node-1"));
        }
    }
}
