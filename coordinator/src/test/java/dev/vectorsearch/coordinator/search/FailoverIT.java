package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.shardnode.testing.TestShardNode;
import io.grpc.Deadline;
import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Step 35: a crashed replica is marked down on UNAVAILABLE, the partition retried on the other one; recovery on heartbeat. */
class FailoverIT {
    @TempDir
    Path dir;
    ClusterFixture c;
    NodeStats stats;
    NodeHealth health;
    ScatterGather sg;

    @BeforeEach
    void up() throws Exception {
        c = new ClusterFixture(dir, 1500);
        stats = new NodeStats();
        health = new NodeHealth(2_000_000_000L, System::nanoTime);
        sg = new ScatterGather(c.view, c.channels, new PowerOfTwoChoicesSelector(stats), stats, health, 0, Duration.ofMillis(5),
                Duration.ofMillis(500), null, null, new SimpleMeterRegistry());
    }

    @AfterEach
    void down() throws Exception {
        c.close();
    }

    int query(Random rnd) {
        return sg.search(ClusterFixture.unit(rnd), 10, Deadline.after(1, TimeUnit.SECONDS)).missingPartitions().size();
    }

    @Test
    void crashedReplicaIsRetriedAroundMarkedDownAndUsedAgainWhenItsHeartbeatReturns() throws Exception {
        // round robin, so node-1 is certainly picked (sequential p2c may happen to prefer the other holders, step 33)
        sg = new ScatterGather(c.view, c.channels, new RoundRobinSelector(), stats, health, 0, Duration.ofMillis(5),
                Duration.ofMillis(500), null, null, new SimpleMeterRegistry());
        Random rnd = new Random(1);
        for (int i = 0; i < 50; i++) query(rnd); // warm up: both holders of every partition have stats
        TestShardNode one = c.nodes.get("node-1");
        int port = one.grpc.port();
        one.crash(); // port closed now; its Redis key lives on for up to the TTL (600 ms here)
        assertTrue(c.view.view().isLive("node-1"), "membership has not noticed yet");

        int missing = 0;
        for (int i = 0; i < 100; i++) missing += query(rnd);
        System.out.printf("node-1 crashed: 100 queries, missing partitions %d, retries %.0f, marked down %.0f%n",
                missing, sg.retries.count(), sg.markedDown.count());
        assertEquals(0, missing, "every partition still answered by its other replica");
        assertTrue(sg.retries.count() >= 1 && sg.retries.count() <= 3, "after the first UNAVAILABLE node-1 is avoided: " + sg.retries.count());
        assertEquals(1, sg.markedDown.count());

        ClusterFixture.waitUntil(() -> !c.view.view().isLive("node-1"), 3000); // TTL expired
        TestShardNode restarted = new TestShardNode("node-1", ClusterFixture.DIM, dir.resolve("node-1"), c.uri, port);
        c.nodes.put("node-1", restarted);
        ClusterFixture.waitUntil(() -> c.view.view().isLive("node-1"), 3000); // heartbeat back
        for (int i = 0; i < 300 && restarted.searches() == 0; i++) {
            assertEquals(0, query(rnd));
            Thread.sleep(10);
        }
        System.out.println("node-1 restarted: searches served by node-1 again = " + restarted.searches());
        assertTrue(restarted.searches() > 0, "the restarted node takes traffic again");
    }

    @Test
    void aReplicaThatFailsFastDoesNotAttractTraffic() {
        c.nodes.get("node-2").searchFailure = Status.INTERNAL.withDescription("broken disk");
        Random rnd = new Random(2);
        int missing = 0;
        for (int i = 0; i < 200; i++) missing += query(rnd);
        int served = c.nodes.get("node-2").searches();
        System.out.printf("node-2 fails every search with INTERNAL in ~0 ms: 200 queries, missing %d, node-2 got %d requests, retries %.0f%n",
                missing, served, sg.retries.count());
        assertEquals(0, missing);
        // 200 queries x 2 partitions held by node-2 = 400 chances; failures are recorded as slow, so p2c stays away
        assertTrue(served < 40, "node-2 got " + served + " requests");
    }
}
