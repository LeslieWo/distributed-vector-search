package dev.vectorsearch.shardnode;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Whole application restarted on the same data dir: what was acknowledged before must be served after. */
class ShardNodeRestartIT {
    @TempDir
    Path dataDir;

    ConfigurableApplicationContext boot() {
        return new SpringApplicationBuilder(ShardNodeApplication.class).run(
                "--shard.grpc-port=0", "--server.port=0", "--shard.dimension=4",
                "--shard.initial-capacity=16", "--shard.data-dir=" + dataDir);
    }

    static <T> T call(ConfigurableApplicationContext ctx, Function<ShardServiceGrpc.ShardServiceBlockingStub, T> fn) {
        ManagedChannel ch = NettyChannelBuilder.forAddress("localhost", ctx.getBean(GrpcServerLifecycle.class).port()).usePlaintext().build();
        try {
            return fn.apply(ShardServiceGrpc.newBlockingStub(ch));
        } finally {
            ch.shutdownNow();
        }
    }

    static UpsertRequest batch(int from, int to) {
        UpsertRequest.Builder b = UpsertRequest.newBuilder();
        for (int i = from; i < to; i++) {
            List<Float> v = new ArrayList<>(List.of(0f, 0f, 0f, 0f));
            v.set(i % 4, 1f);
            v.set((i / 4) % 4, v.get((i / 4) % 4) + 0.5f);
            b.addChunks(ChunkVector.newBuilder().setChunkId("d" + i + "#0").setPartition("node-1").addAllVector(v).setText("t" + i));
        }
        return b.build();
    }

    @Test
    void gracefulRestartThenSecondRestartKeepsEverything() {
        try (ConfigurableApplicationContext first = boot()) {
            assertEquals(40, call(first, s -> s.upsert(batch(0, 40))).getUpserted());
        } // graceful: gRPC stops, then final snapshot
        try (ConfigurableApplicationContext second = boot()) {
            assertEquals(40L, call(second, s -> s.health(HealthRequest.getDefaultInstance())).getPartitionSizesOrThrow("node-1"));
            call(second, s -> s.upsert(batch(40, 50)));
            var hit = call(second, s -> s.search(ShardSearchRequest.newBuilder().setPartition("node-1").setK(1)
                    .addAllVector(List.of(1f, 0f, 0f, 0f)).build())).getHits(0);
            assertEquals(1.5f, hit.getScore(), 1e-5f); // d0/d16/d32/d48 = (1.5, 0, 0, 0), not unit length on purpose
        }
        try (ConfigurableApplicationContext third = boot()) {
            assertEquals(50L, call(third, s -> s.health(HealthRequest.getDefaultInstance())).getPartitionSizesOrThrow("node-1"));
        }
    }
}
