package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.RedisClusterView;
import dev.vectorsearch.common.cluster.RingStore;
import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.common.ring.Placement;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.shardnode.testing.TestShardNode;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;
import org.testcontainers.containers.GenericContainer;

/** Redis + 3 real shard nodes on a ring + N random unit vectors written to both holders of their partition. */
final class ClusterFixture implements AutoCloseable {
    static final int DIM = 16;
    final GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
    final String uri;
    final Map<String, TestShardNode> nodes = new LinkedHashMap<>();
    final HashRing ring = HashRing.of(List.of("node-1", "node-2", "node-3"));
    final RedisClusterView view;
    final ChannelPool channels = new ChannelPool(a -> NettyChannelBuilder.forTarget(a).usePlaintext().build());
    final Map<String, float[]> vectors = new LinkedHashMap<>();

    ClusterFixture(Path dir, int n) throws Exception {
        redis.start();
        uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
        for (String id : ring.nodes()) nodes.put(id, new TestShardNode(id, DIM, dir.resolve(id), uri));
        try (RingStore store = new RingStore(uri)) {
            store.initIfAbsent(ring.nodes());
        }
        view = new RedisClusterView(uri, Duration.ofMillis(100));
        waitUntil(() -> view.view().live().size() == 3, 5000);
        Random rnd = new Random(1);
        for (int i = 0; i < n; i++) vectors.put(i + "#0", unit(rnd));
        Map<String, Map<String, List<String>>> plan = Placement.byNode(ring, vectors.keySet(), id -> id);
        plan.forEach((node, byPartition) -> {
            UpsertRequest.Builder req = UpsertRequest.newBuilder();
            byPartition.forEach((partition, ids) -> ids.forEach(id -> {
                ChunkVector.Builder c = ChunkVector.newBuilder().setChunkId(id).setPartition(partition).setText("text of " + id);
                for (float x : vectors.get(id)) c.addVector(x);
                req.addChunks(c);
            }));
            ShardServiceGrpc.newBlockingStub(channels.get(nodes.get(node).address())).upsert(req.build());
        });
    }

    static float[] unit(Random rnd) {
        float[] v = new float[DIM];
        double norm = 0;
        for (int i = 0; i < DIM; i++) {
            v[i] = (float) rnd.nextGaussian();
            norm += v[i] * v[i];
        }
        for (int i = 0; i < DIM; i++) v[i] /= (float) Math.sqrt(norm);
        return v;
    }

    /** Exact top-k ids by dot product over everything written. */
    List<String> bruteForce(float[] q, int k) {
        List<Map.Entry<String, float[]>> all = new ArrayList<>(vectors.entrySet());
        all.sort(Comparator.comparingDouble((Map.Entry<String, float[]> e) -> -dot(q, e.getValue())));
        return all.subList(0, k).stream().map(Map.Entry::getKey).toList();
    }

    static double dot(float[] a, float[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }

    static void waitUntil(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > end) throw new AssertionError("condition not met in " + timeoutMs + " ms");
            Thread.sleep(20);
        }
    }

    @Override
    public void close() throws Exception {
        view.close();
        channels.close();
        for (TestShardNode n : nodes.values()) n.close();
        redis.stop();
    }
}
