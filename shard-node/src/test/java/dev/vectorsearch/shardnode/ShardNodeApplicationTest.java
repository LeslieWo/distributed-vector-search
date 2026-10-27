package dev.vectorsearch.shardnode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import dev.vectorsearch.shardnode.metrics.ShardMetrics;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

/** Boots the real Spring app with a real Netty gRPC server on a random port and calls it over TCP. */
// Spring Boot disables metric exporters in tests by default; re-enable the Prometheus one.
@AutoConfigureObservability(tracing = false)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"shard.grpc-port=0", "shard.dimension=8", "shard.initial-capacity=64",
        "shard.data-dir=build/test-data/${random.uuid}", "shard.redis-uri="})
class ShardNodeApplicationTest {

    @Autowired
    GrpcServerLifecycle grpc;

    @Autowired
    ShardMetrics metrics;

    @Autowired
    TestRestTemplate http;

    @Test
    void upsertAndSearchOverRealGrpc() {
        ManagedChannel ch = NettyChannelBuilder.forAddress("localhost", grpc.port()).usePlaintext().build();
        try {
            var stub = ShardServiceGrpc.newBlockingStub(ch);
            UpsertRequest.Builder up = UpsertRequest.newBuilder();
            for (int i = 0; i < 8; i++) {
                Float[] v = new Float[8];
                java.util.Arrays.fill(v, 0f);
                v[i] = 1f;
                up.addChunks(ChunkVector.newBuilder().setChunkId("d" + i + "#0").setPartition("node-1")
                        .addAllVector(java.util.List.of(v)).setText("doc " + i));
            }
            assertEquals(8, stub.upsert(up.build()).getUpserted());

            Float[] q = new Float[8];
            java.util.Arrays.fill(q, 0f);
            q[3] = 1f;
            var resp = stub.search(ShardSearchRequest.newBuilder().setPartition("node-1").setK(3)
                    .addAllVector(java.util.List.of(q)).build());
            assertEquals("d3#0", resp.getHits(0).getChunkId());
            assertEquals("doc 3", resp.getHits(0).getText());
            assertEquals(8L, stub.health(HealthRequest.getDefaultInstance()).getPartitionSizesOrDefault("node-1", 0));

            // Step 11: the same calls show up in the Prometheus scrape
            metrics.refresh();
            String scrape = http.getForObject("/actuator/prometheus", String.class);
            assertTrue(scrape.contains("vs_shard_rpc_seconds_bucket{"), "latency histogram buckets");
            assertTrue(scrape.matches("(?s).*vs_shard_rpc_seconds_count\\{[^}]*method=\"Search\"[^}]*status=\"OK\"[^}]*} 1\n.*"), "search count");
            long searchBuckets = scrape.lines().filter(l -> l.startsWith("vs_shard_rpc_seconds_bucket{") && l.contains("method=\"Search\"")).count();
            assertTrue(searchBuckets > 20, "histogram buckets for p50/p99 via histogram_quantile: " + searchBuckets);
            assertTrue(scrape.matches("(?s).*vs_shard_index_size_vectors\\{[^}]*partition=\"node-1\"[^}]*} 8\\.0.*"), "index size gauge");
            assertTrue(scrape.matches("(?s).*vs_shard_upserted_chunks_total\\{[^}]*} 8\\.0.*"), "upsert counter");
        } finally {
            ch.shutdownNow();
        }
    }
}
