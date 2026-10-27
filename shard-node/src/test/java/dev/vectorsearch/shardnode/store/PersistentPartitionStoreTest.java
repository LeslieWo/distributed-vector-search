package dev.vectorsearch.shardnode.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.shardnode.ShardNodeProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

// One unexplained 303 s stall was seen once (BUILDLOG step 12); fail fast instead of hanging the build.
@Timeout(30)
class PersistentPartitionStoreTest {
    @TempDir
    Path dir;

    ShardNodeProperties props() {
        return props(0);
    }

    ShardNodeProperties props(long snapshotWalBytes) {
        return ShardNodeProperties.forTest("node-1", 3, dir, Duration.ofHours(1), snapshotWalBytes);
    }

    static PartitionStore.Entry e(String id, String partition, float... v) {
        return new PartitionStore.Entry(id, partition, v, "t-" + id);
    }

    @Test
    void gracefulCloseSnapshotsAndRestartRestores() throws Exception {
        PersistentPartitionStore s = new PersistentPartitionStore(props());
        s.upsert(List.of(e("a#0", "node-1", 1, 0, 0), e("b#0", "node-3", 0, 1, 0)));
        s.close();
        assertTrue(Files.exists(dir.resolve("node-1.hnsw")));
        assertEquals(0, Files.size(dir.resolve("wal.log")));

        PersistentPartitionStore restarted = new PersistentPartitionStore(props());
        assertEquals(Map.of("node-1", 1L, "node-3", 1L), restarted.sizes());
        assertEquals("t-a#0", restarted.search("node-1", new float[] {1, 0, 0}, 1, 0).get(0).text());
        restarted.close();
    }

    @Test
    void crashAfterSnapshotRecoversLaterWritesFromWal() throws Exception {
        PersistentPartitionStore s = new PersistentPartitionStore(props());
        s.upsert(List.of(e("a#0", "p", 1, 0, 0)));
        s.snapshot();
        s.upsert(List.of(e("b#0", "p", 0, 1, 0), e("c#0", "p", 0, 0, 1)));
        s.delete("p", List.of("a#0"));
        // simulate kill -9: no close(), no final snapshot
        PersistentPartitionStore recovered = new PersistentPartitionStore(props());
        assertEquals(Map.of("p", 2L), recovered.sizes());
        assertEquals("b#0", recovered.search("p", new float[] {0, 1, 0}, 1, 0).get(0).id());
        // the replayed writes must be snapshotted on close even though nothing new was written
        recovered.close();
        assertEquals(0, Files.size(dir.resolve("wal.log")));
        PersistentPartitionStore again = new PersistentPartitionStore(props());
        assertEquals(Map.of("p", 2L), again.sizes());
        again.close();
    }

    @Test
    void replayingWritesAlreadyInSnapshotIsHarmless() throws Exception {
        PersistentPartitionStore s = new PersistentPartitionStore(props());
        s.upsert(List.of(e("a#0", "p", 1, 0, 0), e("b#0", "p", 0, 1, 0)));
        // crash between "snapshot files written" and "WAL truncated": copy the WAL, snapshot, put the WAL back
        byte[] walBefore = Files.readAllBytes(dir.resolve("wal.log"));
        s.snapshot();
        Files.write(dir.resolve("wal.log"), walBefore);
        PersistentPartitionStore recovered = new PersistentPartitionStore(props());
        assertEquals(Map.of("p", 2L), recovered.sizes()); // no duplicates
        recovered.close();
    }

    @Test
    void tornWalTailIsIgnored() throws Exception {
        PersistentPartitionStore s = new PersistentPartitionStore(props());
        s.upsert(List.of(e("a#0", "p", 1, 0, 0)));
        // half-written record, as if the process died in the middle of an append
        Files.write(dir.resolve("wal.log"), new byte[] {1, 0, 0, 0, 99, 42, 42}, StandardOpenOption.APPEND);
        PersistentPartitionStore recovered = new PersistentPartitionStore(props());
        assertEquals(Map.of("p", 1L), recovered.sizes());
        recovered.close();
    }

    @Test
    void walOverThresholdTriggersEarlySnapshot() throws Exception {
        PersistentPartitionStore s = new PersistentPartitionStore(props(300)); // one 3-d record is ~50 bytes
        for (int i = 0; i < 10; i++) s.upsert(List.of(e("x" + i + "#0", "p", 1, i, 0)));
        long deadline = System.currentTimeMillis() + 10_000;
        while (!Files.exists(dir.resolve("p.hnsw")) && System.currentTimeMillis() < deadline) Thread.sleep(20);
        assertTrue(Files.exists(dir.resolve("p.hnsw")), "interval is 1 h, only the WAL-size trigger can have written this");
        assertTrue(Files.size(dir.resolve("wal.log")) < 300 * 2, "WAL was truncated by the early snapshot");
        s.close();
        assertEquals(Map.of("p", 10L), new PersistentPartitionStore(props()).sizes());
    }

    @Test
    void onlyChangedPartitionsAreRewritten() throws Exception {
        PersistentPartitionStore s = new PersistentPartitionStore(props());
        s.upsert(List.of(e("a#0", "pa", 1, 0, 0), e("b#0", "pb", 0, 1, 0)));
        s.snapshot();
        var pbTime = Files.getLastModifiedTime(dir.resolve("pb.hnsw"));
        var paTime = Files.getLastModifiedTime(dir.resolve("pa.hnsw"));
        Thread.sleep(20);
        s.upsert(List.of(e("c#0", "pa", 0, 0, 1)));
        s.snapshot();
        assertEquals(pbTime, Files.getLastModifiedTime(dir.resolve("pb.hnsw")));
        assertTrue(Files.getLastModifiedTime(dir.resolve("pa.hnsw")).compareTo(paTime) > 0);
        s.close();
        assertEquals(Map.of("pa", 2L, "pb", 1L), new PersistentPartitionStore(props()).sizes());
    }
}
