package dev.vectorsearch.shardnode.testing;

import dev.vectorsearch.common.cluster.Heartbeater;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardSearchResponse;
import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import dev.vectorsearch.shardnode.grpc.ShardGrpcService;
import dev.vectorsearch.shardnode.store.PersistentPartitionStore;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real shard node (persistent store + gRPC + Redis heartbeat) assembled without Spring, for multi-node tests
 * in other modules. Searches can be slowed down or failed on demand, and are counted per partition.
 */
public final class TestShardNode implements AutoCloseable {
    public final String id;
    public final PersistentPartitionStore store;
    public final GrpcServerLifecycle grpc;
    public final Heartbeater heartbeat;
    public final Map<String, AtomicInteger> searchesPerPartition = new ConcurrentHashMap<>();
    /** added to every Search before it runs (simulates a slow or overloaded node) */
    public volatile long searchDelayMillis;
    /** if set, an extra random delay per Search in ms (simulates GC pauses / noisy neighbours: tail latency) */
    public volatile java.util.function.LongSupplier randomDelayMillis;
    /** Searches the node skipped because the caller had already cancelled or timed out (step 32) */
    public final AtomicInteger skippedExpired = new AtomicInteger();
    /** every Search fails with this status (null = serve normally) */
    public volatile Status searchFailure;
    /** time left until the caller's deadline when the last Search arrived, ms (-1 = no deadline) */
    public volatile long lastDeadlineMillis = -1;

    public TestShardNode(String id, int dim, Path dataDir, String redisUri) {
        this(id, dim, dataDir, redisUri, 0);
    }

    public TestShardNode(String id, int dim, Path dataDir, String redisUri, int port) {
        this.id = id;
        this.store = new PersistentPartitionStore(ShardNodeProperties.forTest(id, dim, dataDir, Duration.ofHours(1), 0));
        ShardGrpcService service = new ShardGrpcService(store) {
            @Override
            public void search(ShardSearchRequest req, StreamObserver<ShardSearchResponse> out) {
                searchesPerPartition.computeIfAbsent(req.getPartition(), p -> new AtomicInteger()).incrementAndGet();
                Deadline d = Context.current().getDeadline();
                lastDeadlineMillis = d == null ? -1 : d.timeRemaining(TimeUnit.MILLISECONDS);
                if (searchFailure != null) {
                    out.onError(searchFailure.asRuntimeException());
                    return;
                }
                long delay = searchDelayMillis + (randomDelayMillis == null ? 0 : randomDelayMillis.getAsLong());
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.search(req, out);
            }
        };
        service.setOnExpired(skippedExpired::incrementAndGet);
        this.grpc = new GrpcServerLifecycle(port, 8, List.of(service));
        grpc.start();
        this.heartbeat = new Heartbeater(redisUri, id, "localhost:" + grpc.port(), Duration.ofMillis(200), Duration.ofMillis(600));
    }

    public String address() {
        return "localhost:" + grpc.port();
    }

    public int searches() {
        return searchesPerPartition.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    /** kill -9 as seen from outside: heartbeat stops (key expires), port closes, nothing is flushed. */
    public void crash() {
        heartbeat.stopBeating();
        grpc.stop();
    }

    @Override
    public void close() throws Exception {
        heartbeat.close();
        if (grpc.isRunning()) grpc.stop();
        store.close();
    }
}
