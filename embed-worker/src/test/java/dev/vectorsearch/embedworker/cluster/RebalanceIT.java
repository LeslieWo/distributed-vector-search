package dev.vectorsearch.embedworker.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.Rebalancer;
import dev.vectorsearch.common.cluster.RedisClusterView;
import dev.vectorsearch.common.cluster.RingStore;
import dev.vectorsearch.common.cluster.ViewAcker;
import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.common.ring.Partition;
import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import dev.vectorsearch.embedworker.shard.RingShardWriter;
import dev.vectorsearch.embedworker.shard.ShardWriteException;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.ScanRequest;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardSearchResponse;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;

/**
 * Steps 27-28: add a 4th node while writes keep flowing, then remove a dead node. Afterwards every chunk must
 * be stored exactly on the two holders of its partition under the new ring (nowhere else) and be searchable.
 */
class RebalanceIT {
    static final int DIM = 8;

    @TempDir
    Path dir;
    GenericContainer<?> redis;
    String uri;
    final Map<String, TestShardNode> nodes = new TreeMap<>();
    ChannelPool pool;
    RedisClusterView view;
    RingShardWriter writer;
    ViewAcker acker;

    @BeforeEach
    void up() {
        redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        redis.start();
        uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
        for (String n : List.of("node-1", "node-2", "node-3", "node-4")) nodes.put(n, new TestShardNode(n, DIM, dir.resolve(n), uri));
        try (RingStore store = new RingStore(uri)) {
            store.initIfAbsent(List.of("node-1", "node-2", "node-3"));
        }
        pool = new ChannelPool(a -> NettyChannelBuilder.forTarget(a).usePlaintext().maxInboundMessageSize(64 << 20).build());
        view = new RedisClusterView(uri, Duration.ofMillis(100));
        writer = new RingShardWriter(view, new ChannelPool(a -> NettyChannelBuilder.forTarget(a).usePlaintext().build()), 5000);
        acker = new ViewAcker(uri, ViewAcker.WRITER, "test-writer", writer::ackState, Duration.ofMillis(200), Duration.ofMillis(600));
    }

    @AfterEach
    void down() throws Exception {
        acker.close();
        writer.close();
        view.close();
        pool.close();
        for (TestShardNode n : nodes.values()) n.close();
        redis.stop();
    }

    static EmbeddedChunk chunk(String id) {
        Random r = new Random(id.hashCode());
        float[] v = new float[DIM];
        double norm = 0;
        for (int i = 0; i < DIM; i++) {
            v[i] = (float) r.nextGaussian();
            norm += v[i] * v[i];
        }
        for (int i = 0; i < DIM; i++) v[i] /= (float) Math.sqrt(norm);
        return new EmbeddedChunk(id, v, "text " + id);
    }

    void writeWithRetry(List<EmbeddedChunk> batch) throws InterruptedException {
        while (true) {
            try {
                writer.write(batch);
                return;
            } catch (ShardWriteException e) {
                if (!e.retriable()) throw e;
                Thread.sleep(50);
            }
        }
    }

    List<String> writeChunks(String prefix, int n) throws InterruptedException {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i += 100) {
            List<EmbeddedChunk> batch = new ArrayList<>();
            for (int j = i; j < Math.min(n, i + 100); j++) batch.add(chunk(prefix + j + "#0"));
            writeWithRetry(batch);
            batch.forEach(c -> ids.add(c.chunkId()));
        }
        return ids;
    }

    ShardServiceGrpc.ShardServiceBlockingStub stub(String node) {
        return ShardServiceGrpc.newBlockingStub(pool.get(nodes.get(node).address()));
    }

    /** chunk id -> set of "node/partition" where it is stored, over the given (live) nodes. */
    Map<String, Set<String>> locations(List<String> liveNodes) {
        Map<String, Set<String>> where = new HashMap<>();
        for (String n : liveNodes) {
            for (String partition : stub(n).health(HealthRequest.getDefaultInstance()).getPartitionSizesMap().keySet()) {
                stub(n).scan(ScanRequest.newBuilder().setPartition(partition).build()).forEachRemaining(m -> {
                    for (ChunkVector c : m.getChunksList()) where.computeIfAbsent(c.getChunkId(), k -> new HashSet<>()).add(n + "/" + partition);
                });
            }
        }
        return where;
    }

    void assertExactlyOnItsTwoHolders(HashRing ring, List<String> ids, List<String> liveNodes) {
        Map<String, Set<String>> where = locations(liveNodes);
        for (String id : ids) {
            Partition p = ring.partitionOf(id);
            Set<String> expected = new HashSet<>();
            for (String h : p.holders()) if (liveNodes.contains(h)) expected.add(h + "/" + p.id());
            assertEquals(expected, where.get(id), id);
        }
        assertEquals(new HashSet<>(ids), where.keySet(), "no stray chunks");
        // searchable: each chunk's own vector finds it first in its partition, on either holder
        for (int i = 0; i < ids.size(); i += 37) {
            String id = ids.get(i);
            EmbeddedChunk c = chunk(id);
            ShardSearchRequest.Builder q = ShardSearchRequest.newBuilder().setPartition(ring.partitionOf(id).id()).setK(1);
            for (float x : c.vector()) q.addVector(x);
            for (String h : ring.partitionOf(id).holders()) {
                if (!liveNodes.contains(h)) continue;
                ShardSearchResponse r = stub(h).search(q.build());
                assertEquals(id, r.getHits(0).getChunkId(), "search on " + h);
            }
        }
    }

    @Test
    void addANodeUnderWriteLoadThenRemoveADeadOne() throws Exception {
        List<String> ids = new ArrayList<>(writeChunks("a", 3000));
        HashRing three = HashRing.of(List.of("node-1", "node-2", "node-3"));
        assertExactlyOnItsTwoHolders(three, ids, List.of("node-1", "node-2", "node-3", "node-4"));

        // keep writing new chunks while node-4 joins
        AtomicBoolean stop = new AtomicBoolean();
        List<String> during = java.util.Collections.synchronizedList(new ArrayList<>());
        Thread load = Thread.ofPlatform().start(() -> {
            try {
                for (int i = 0; !stop.get(); i++) {
                    List<EmbeddedChunk> batch = new ArrayList<>();
                    for (int j = 0; j < 20; j++) batch.add(chunk("b" + (i * 20 + j) + "#0"));
                    writeWithRetry(batch);
                    batch.forEach(c -> during.add(c.chunkId()));
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Rebalancer.Report add;
        try (RingStore store = new RingStore(uri)) {
            Rebalancer rb = new Rebalancer(store, pool, Duration.ofSeconds(30), Duration.ofMillis(500), new PrintStream(System.out));
            add = rb.rebalance(List.of("node-1", "node-2", "node-3", "node-4"));
        }
        stop.set(true);
        load.join();
        ids.addAll(during);
        HashRing four = HashRing.of(List.of("node-1", "node-2", "node-3", "node-4"));
        System.out.printf("add node-4: %s; %d chunks written during the rebalance%n", add, during.size());
        assertTrue(during.size() > 100, "writes really overlapped the rebalance: " + during.size());
        // only chunks whose partition changes are copied: at least the original ones that move, never all of them
        long originalMovers = ids.subList(0, 3000).stream().filter(id -> !three.partitionOf(id).equals(four.partitionOf(id))).count();
        assertTrue(add.chunksMoved() >= originalMovers && add.chunksMoved() < 3000 + during.size(), add + " vs " + originalMovers);
        assertExactlyOnItsTwoHolders(four, ids, List.of("node-1", "node-2", "node-3", "node-4"));

        // node-2 dies for good; remove it, its partitions are re-replicated from the surviving holders
        nodes.get("node-2").crash();
        Thread.sleep(1000); // heartbeat TTL 600 ms
        try (RingStore store = new RingStore(uri)) {
            Rebalancer rb = new Rebalancer(store, pool, Duration.ofSeconds(30), Duration.ofMillis(500), new PrintStream(System.out));
            System.out.println("remove node-2: " + rb.rebalance(List.of("node-1", "node-3", "node-4")));
            HashRing without2 = HashRing.of(List.of("node-1", "node-3", "node-4"));
            assertExactlyOnItsTwoHolders(without2, ids, List.of("node-1", "node-3", "node-4"));
            rb.partitionSizes().forEach((p, sizes) -> assertEquals(1, new HashSet<>(sizes.values()).size(), p + " " + sizes));
        }
    }
}
