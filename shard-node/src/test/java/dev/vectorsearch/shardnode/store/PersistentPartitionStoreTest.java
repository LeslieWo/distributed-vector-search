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
import org.junit.jupiter.api.io.TempDir;

class PersistentPartitionStoreTest {
    @TempDir
    Path dir;

    ShardNodeProperties props() {
        return new ShardNodeProperties("node-1", 0, 3, 16, 100, 32, 16, 100, 2, dir, Duration.ofHours(1));
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
}
