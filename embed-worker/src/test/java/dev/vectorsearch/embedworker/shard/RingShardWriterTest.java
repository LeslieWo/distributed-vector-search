package dev.vectorsearch.embedworker.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.ClusterView;
import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.embedworker.it.FakeShard;
import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Real gRPC fake shards: each chunk must reach exactly the two holders of its partition. */
class RingShardWriterTest {
    final Map<String, FakeShard> shards = new LinkedHashMap<>();
    final AtomicReference<ClusterView> view = new AtomicReference<>();
    RingShardWriter writer;

    @BeforeEach
    void up() throws Exception {
        for (String n : List.of("node-1", "node-2", "node-3", "node-4")) shards.put(n, new FakeShard());
        view.set(view(HashRing.of(List.of("node-1", "node-2", "node-3")), null, shards.keySet()));
        writer = new RingShardWriter(view::get,
                new ChannelPool(a -> NettyChannelBuilder.forTarget(a).usePlaintext().build()), 2000);
    }

    @AfterEach
    void down() {
        writer.close();
        shards.values().forEach(FakeShard::close);
    }

    ClusterView view(HashRing ring, HashRing pending, Set<String> live) {
        Map<String, String> addr = new LinkedHashMap<>();
        for (String n : live) addr.put(n, shards.get(n).target());
        return new ClusterView(1, ring, pending, addr);
    }

    static List<EmbeddedChunk> chunks(int n) {
        List<EmbeddedChunk> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.add(new EmbeddedChunk(i + "#0", new float[] {1, 0, 0}, "t" + i));
        return out;
    }

    long totalUpserts() {
        return shards.values().stream().mapToLong(s -> s.totalUpserts.get()).sum();
    }

    @Test
    void eachChunkGoesToBothHoldersOfItsPartition() {
        writer.write(chunks(500));
        HashRing ring = view.get().ring();
        for (int i = 0; i < 500; i++) {
            String id = i + "#0";
            Set<String> holdersSeen = new HashSet<>();
            shards.forEach((node, s) -> {
                if (s.partitionsOf.containsKey(id)) {
                    holdersSeen.add(node);
                    assertEquals(Set.of(ring.partitionOf(id).id()), s.partitionsOf.get(id));
                }
            });
            assertEquals(new HashSet<>(ring.partitionOf(id).holders()), holdersSeen, id);
        }
        assertEquals(1000, totalUpserts()); // 2 copies each, nothing more
        assertEquals(0, shards.get("node-4").totalUpserts.get()); // not in the ring
    }

    @Test
    void aTargetWithoutHeartbeatFailsTheBatchRetriablyWithoutSendingAnything() {
        view.set(view(view.get().ring(), null, Set.of("node-1", "node-2")));
        ShardWriteException e = assertThrows(ShardWriteException.class, () -> writer.write(chunks(50)));
        assertTrue(e.retriable());
        assertEquals(0, totalUpserts());
    }

    @Test
    void unavailableNodeIsRetriableAndInvalidArgumentIsNot() {
        shards.get("node-2").down = true;
        assertTrue(assertThrows(ShardWriteException.class, () -> writer.write(chunks(50))).retriable());
        shards.get("node-2").down = false;
        shards.values().forEach(s -> s.invalid = c -> true);
        assertFalse(assertThrows(ShardWriteException.class, () -> writer.write(chunks(50))).retriable());
    }

    @Test
    void duringARebalanceChunksAlsoGoToTheirOwnersUnderThePendingRing() {
        HashRing old = HashRing.of(List.of("node-1", "node-2", "node-3"));
        HashRing next = HashRing.of(List.of("node-1", "node-2", "node-3", "node-4"));
        view.set(view(old, next, shards.keySet()));
        writer.write(chunks(300));
        for (int i = 0; i < 300; i++) {
            String id = i + "#0";
            for (String n : old.partitionOf(id).holders()) assertTrue(shards.get(n).partitionsOf.get(id).contains(old.partitionOf(id).id()));
            for (String n : next.partitionOf(id).holders()) assertTrue(shards.get(n).partitionsOf.get(id).contains(next.partitionOf(id).id()));
        }
        assertTrue(shards.get("node-4").totalUpserts.get() > 0);
    }

    /**
     * A node that was unreachable for a while and comes back must be usable at once. Without dropping its channel
     * when the heartbeat vanished, gRPC's reconnect backoff kept failing calls for many seconds (BUILDLOG step 26).
     */
    @Test
    void aNodeThatComesBackIsWrittenToImmediately() throws Exception {
        int port = shards.get("node-2").port();
        shards.get("node-2").close(); // crash: connection refused from now on
        long until = System.currentTimeMillis() + 4000; // keep failing for 4 s so the channel backoff grows
        while (System.currentTimeMillis() < until) {
            assertThrows(ShardWriteException.class, () -> writer.write(chunks(30)));
            Thread.sleep(50);
        }
        view.set(view(view.get().ring(), null, Set.of("node-1", "node-3", "node-4"))); // heartbeat expired
        assertThrows(ShardWriteException.class, () -> writer.write(chunks(30)));
        shards.put("node-2", new FakeShard(port)); // restarted at the same address, heartbeat is back
        view.set(view(view.get().ring(), null, Set.of("node-1", "node-2", "node-3", "node-4")));
        long t0 = System.nanoTime();
        writer.write(chunks(30)); // must not throw
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 1000);
        assertTrue(shards.get("node-2").totalUpserts.get() > 0);
    }
}
