package dev.vectorsearch.embedworker.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.vectorsearch.common.ring.HashRing;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Step 23: Kafka -> worker -> each chunk on both holders of its ring partition; a replica outage blocks commits (ack=ALL). */
class RingRoutingIT extends WorkerIT {

    @Test
    void workerWritesEveryChunkToItsTwoReplicasAndCommitsOnlyAfterBothAcked() throws Exception {
        try (FakeShard two = new FakeShard(); FakeShard three = new FakeShard()) {
            Map<String, FakeShard> nodes = Map.of("node-1", shard, "node-2", two, "node-3", three);
            String spec = "node-1=" + shard.target() + ",node-2=" + two.target() + ",node-3=" + three.target();
            HashRing ring = HashRing.of(nodes.keySet());
            three.down = true; // one replica down from the start
            List<String> ids = produce("r", 400);
            try (ConfigurableApplicationContext w = startWorker("--worker.static-nodes=" + spec)) {
                await().atMost(Duration.ofSeconds(30)).until(() -> three.rejectedCalls.get() >= 3);
                assertEquals(0, committedTotal("embed-worker"), "ack=ALL: no commit while a replica cannot ack");
                three.down = false;
                await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 400);
            }
            for (String id : ids) {
                Set<String> holders = new HashSet<>();
                nodes.forEach((n, s) -> {
                    if (s.chunkIds().contains(id)) holders.add(n);
                });
                assertEquals(new HashSet<>(ring.partitionOf(id).holders()), holders, id);
            }
        }
    }
}
