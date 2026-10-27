package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.ring.Partition;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.shardnode.testing.TestShardNode;
import io.grpc.Deadline;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Steps 30-31: one request per partition to one replica, merged into the exact global top-k. */
class ScatterGatherIT {
    @TempDir
    Path dir;
    ClusterFixture c;
    ScatterGather sg;

    @BeforeEach
    void up() throws Exception {
        c = new ClusterFixture(dir, 3000);
        sg = new ScatterGather(c.view, c.channels, new RoundRobinSelector(), 200);
    }

    @AfterEach
    void down() throws Exception {
        c.close();
    }

    static Deadline in(long ms) {
        return Deadline.after(ms, TimeUnit.MILLISECONDS);
    }

    @Test
    void eachQueryAsksEveryPartitionExactlyOnceOnOneReplica() {
        Random rnd = new Random(9);
        for (int q = 0; q < 50; q++) sg.search(ClusterFixture.unit(rnd), 10, in(2000));
        for (Partition p : c.ring.partitions()) {
            int total = 0;
            for (String h : p.holders()) {
                TestShardNode n = c.nodes.get(h);
                int calls = n.searchesPerPartition.getOrDefault(p.id(), new java.util.concurrent.atomic.AtomicInteger()).get();
                total += calls;
                assertTrue(calls >= 20 && calls <= 30, "round robin spreads " + p.id() + " evenly: " + h + "=" + calls);
            }
            assertEquals(50, total, p.id() + ": once per query");
        }
    }

    @Test
    void mergedTopKMatchesBruteForceAndCarriesText() {
        Random rnd = new Random(5);
        double recall = 0;
        int queries = 100;
        for (int q = 0; q < queries; q++) {
            float[] query = ClusterFixture.unit(rnd);
            ShardSearcher.Result r = sg.search(query, 10, in(2000));
            assertTrue(r.missingPartitions().isEmpty());
            assertEquals(10, r.hits().size());
            for (int i = 1; i < r.hits().size(); i++) assertTrue(r.hits().get(i - 1).getScore() >= r.hits().get(i).getScore(), "sorted best first");
            for (ScoredChunk h : r.hits()) assertEquals("text of " + h.getChunkId(), h.getText());
            List<String> exact = c.bruteForce(query, 10);
            recall += r.hits().stream().filter(h -> exact.contains(h.getChunkId())).count() / 10.0;
            assertEquals(10, new HashSet<>(r.hits().stream().map(ScoredChunk::getChunkId).toList()).size(), "no duplicates");
        }
        recall /= queries;
        System.out.println("recall@10 vs brute force over 3000 vectors, ef=200: " + recall);
        assertTrue(recall >= 0.98, "recall " + recall);
    }
}
