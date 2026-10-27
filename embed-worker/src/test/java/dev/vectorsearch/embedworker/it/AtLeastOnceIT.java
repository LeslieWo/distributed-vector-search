package dev.vectorsearch.embedworker.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Step 17: consumer group + offsets committed only after the shard acknowledged. */
class AtLeastOnceIT extends WorkerIT {

    @Test
    void everyRecordIsWrittenThenCommitted() throws Exception {
        List<String> ids = produce("d", 500);
        try (ConfigurableApplicationContext w = startWorker()) {
            await().atMost(Duration.ofSeconds(60)).until(() -> shard.chunkIds().size() == 500);
            await().atMost(Duration.ofSeconds(10)).until(() -> committedTotal("embed-worker") == 500);
        }
        assertEquals(new HashSet<>(ids), shard.chunkIds());
    }

    @Test
    void restartResumesFromCommittedOffsetsWithoutRedoingWork() throws Exception {
        produce("a", 300);
        try (ConfigurableApplicationContext w = startWorker()) {
            await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 300);
        } // graceful stop
        produce("b", 200); // arrives while no worker is running
        try (ConfigurableApplicationContext w = startWorker()) {
            await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 500);
        }
        assertEquals(500, shard.chunkIds().size());
        assertEquals(500, shard.totalUpserts.get(), "graceful stop + restart must not reprocess committed records");
    }

    @Test
    void shardOutageIsRetriedAndNothingIsCommittedUntilItRecovers() throws Exception {
        shard.down = true;
        produce("x", 100);
        try (ConfigurableApplicationContext w = startWorker()) {
            await().atMost(Duration.ofSeconds(30)).until(() -> shard.rejectedCalls.get() >= 5);
            assertEquals(0, committedTotal("embed-worker"), "nothing may be committed while the shard is down");
            shard.down = false;
            await().atMost(Duration.ofSeconds(30)).until(() -> committedTotal("embed-worker") == 100);
        }
        Set<String> got = shard.chunkIds();
        assertEquals(100, got.size());
    }
}
