package dev.vectorsearch.embedworker.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.kafka.Topics;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Step 18: replaying already-processed records (what a crash before commit causes) is harmless. */
class DuplicateDeliveryIT extends WorkerIT {

    @Test
    void rewindingTheGroupRedeliversEverythingAndTheShardSeesTheSameIds() throws Exception {
        produce("d", 200);
        try (ConfigurableApplicationContext w = startWorker()) {
            await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 200);
        }
        // Simulate "crashed after writing, before committing": move the group back to offset 0.
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> zero = new HashMap<>();
            for (int p = 0; p < Topics.RAW_CHUNKS_PARTITIONS; p++) zero.put(new TopicPartition(Topics.RAW_CHUNKS, p), new OffsetAndMetadata(0));
            admin.alterConsumerGroupOffsets("embed-worker", zero).all().get();
        }
        try (ConfigurableApplicationContext w = startWorker()) {
            await().atMost(Duration.ofSeconds(60)).until(() -> shard.totalUpserts.get() == 400);
        }
        assertEquals(200, shard.chunkIds().size(), "same 200 ids, now each written twice");
        assertTrue(shard.upsertsPerChunk.values().stream().allMatch(n -> n.get() == 2));
    }

    @Test
    void sameChunkTwiceInOneBatchIsEmbeddedAndWrittenOnce() throws Exception {
        produce("same", 1);
        produce("same", 1); // identical key + value again, lands in the same partition right behind it
        try (ConfigurableApplicationContext w = startWorker("--worker.batch-max-wait=2s")) {
            await().atMost(Duration.ofSeconds(30)).until(() -> committedTotal("embed-worker") == 2);
        }
        assertEquals(1, shard.totalUpserts.get());
    }
}
