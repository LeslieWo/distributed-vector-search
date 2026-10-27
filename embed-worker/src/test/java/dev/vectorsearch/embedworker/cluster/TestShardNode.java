package dev.vectorsearch.embedworker.cluster;

import dev.vectorsearch.common.cluster.Heartbeater;
import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import dev.vectorsearch.shardnode.grpc.ShardGrpcService;
import dev.vectorsearch.shardnode.store.PersistentPartitionStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/** A real shard node (persistent store + gRPC + heartbeat) assembled without Spring, for multi-node tests. */
final class TestShardNode implements AutoCloseable {
    final String id;
    final PersistentPartitionStore store;
    final GrpcServerLifecycle grpc;
    final Heartbeater heartbeat;

    TestShardNode(String id, int dim, Path dataDir, String redisUri) {
        this.id = id;
        this.store = new PersistentPartitionStore(ShardNodeProperties.forTest(id, dim, dataDir, Duration.ofHours(1), 0));
        this.grpc = new GrpcServerLifecycle(0, 4, List.of(new ShardGrpcService(store)));
        grpc.start();
        this.heartbeat = new Heartbeater(redisUri, id, "localhost:" + grpc.port(), Duration.ofMillis(200), Duration.ofMillis(600));
    }

    String address() {
        return "localhost:" + grpc.port();
    }

    /** kill -9 as seen from outside: heartbeat stops (key expires), port closes, nothing is flushed. */
    void crash() {
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
