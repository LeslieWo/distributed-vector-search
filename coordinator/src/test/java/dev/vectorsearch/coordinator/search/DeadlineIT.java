package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.shardnode.testing.TestShardNode;
import io.grpc.Deadline;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Step 32: the query deadline reaches every shard (minus the response reserve, capped by the shard timeout). */
class DeadlineIT {
    @TempDir
    Path dir;
    ClusterFixture c;
    ScatterGather sg;

    @BeforeEach
    void up() throws Exception {
        c = new ClusterFixture(dir, 600);
        sg = new ScatterGather(c.view, c.channels, new RoundRobinSelector(), 0, Duration.ofMillis(20), Duration.ofMillis(500));
    }

    @AfterEach
    void down() throws Exception {
        c.close();
    }

    @Test
    void shardsSeeTheQueryDeadlineMinusTheReserveCappedByTheShardTimeout() {
        float[] q = ClusterFixture.unit(new Random(1));
        sg.search(q, 10, Deadline.after(300, TimeUnit.MILLISECONDS));
        for (TestShardNode n : c.nodes.values()) {
            if (n.searches() == 0) continue;
            System.out.println("query deadline 300 ms, reserve 20 ms: " + n.id + " saw " + n.lastDeadlineMillis + " ms left");
            assertTrue(n.lastDeadlineMillis > 150 && n.lastDeadlineMillis <= 280, n.id + " saw " + n.lastDeadlineMillis + " ms");
        }
        sg.search(q, 10, Deadline.after(10, TimeUnit.SECONDS));
        for (TestShardNode n : c.nodes.values()) {
            if (n.searches() == 0) continue;
            System.out.println("query deadline 10 s, shard timeout 500 ms: " + n.id + " saw " + n.lastDeadlineMillis + " ms left");
            assertTrue(n.lastDeadlineMillis > 300 && n.lastDeadlineMillis <= 500, n.id + " saw " + n.lastDeadlineMillis + " ms (cap 500)");
        }
    }

    @Test
    void aSlowShardCannotHoldTheQueryPastItsDeadline() {
        c.nodes.get("node-1").searchDelayMillis = 400;
        Random rnd = new Random(2);
        int sawMissing = 0;
        for (int i = 0; i < 6; i++) {
            long t0 = System.nanoTime();
            ShardSearcher.Result r = sg.search(ClusterFixture.unit(rnd), 10, Deadline.after(150, TimeUnit.MILLISECONDS));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            System.out.println("node-1 +400 ms, deadline 150 ms: answered in " + ms + " ms, missing " + r.missingPartitions() + ", hits " + r.hits().size());
            assertTrue(ms < 150, "answered in " + ms + " ms, before the 150 ms deadline");
            for (String p : r.missingPartitions()) assertTrue(p.contains("node-1"), "only node-1's partitions can be late: " + p);
            if (!r.missingPartitions().isEmpty()) {
                sawMissing++;
                assertFalse(r.hits().isEmpty(), "the other partitions still answered");
            }
        }
        assertTrue(sawMissing > 0, "round robin sent some partitions to slow node-1");
    }
}
