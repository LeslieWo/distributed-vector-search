package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.coordinator.CoordinatorProperties;
import dev.vectorsearch.embedding.Embedder;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.SearchRequest;
import dev.vectorsearch.proto.v1.SearchResponse;
import dev.vectorsearch.proto.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Step 36: both replicas of a partition down -> answer from the rest, partial=true, missing partitions listed. */
class PartialResultsIT {
    @TempDir
    Path dir;
    ClusterFixture c;
    Server server;
    ManagedChannel channel;
    SearchServiceGrpc.SearchServiceBlockingStub stub;
    final Random rnd = new Random(4);

    @BeforeEach
    void up() throws Exception {
        c = new ClusterFixture(dir, 1500);
        // "embedding" = a random unit vector of the fixture's dimension; this test is about the response, not the model
        Embedder embedder = new Embedder() {
            public int dimension() {
                return ClusterFixture.DIM;
            }

            public float[][] embed(List<String> texts) {
                return texts.stream().map(t -> ClusterFixture.unit(rnd)).toArray(float[][]::new);
            }
        };
        NodeStats stats = new NodeStats();
        ScatterGather sg = new ScatterGather(c.view, c.channels, new PowerOfTwoChoicesSelector(stats), stats, new NodeHealth(), 0,
                Duration.ofMillis(5), Duration.ofMillis(500), null, null, new SimpleMeterRegistry());
        CoordinatorProperties props = new CoordinatorProperties(0, 4, null, null, "hashing", null, 10, 100, 0, null, null, null, null,
                false, null, null, 0, false, null, null);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(new SearchServiceImpl(props, embedder, sg, null, new SimpleMeterRegistry())).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        stub = SearchServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void down() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
        c.close();
    }

    SearchResponse search() {
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS).search(SearchRequest.newBuilder().setText("q").setK(10).build());
    }

    @Test
    void oneNodeDownIsInvisibleBothReplicasDownIsFlagged() throws Exception {
        c.nodes.get("node-1").crash();
        for (int i = 0; i < 20; i++) {
            SearchResponse r = search();
            assertFalse(r.getPartial(), "every partition still has a live replica");
            assertEquals(10, r.getHitsCount());
        }
        c.nodes.get("node-2").crash(); // node-1.node-2 has no replica left
        for (int i = 0; i < 20; i++) {
            SearchResponse r = search();
            assertTrue(r.getPartial());
            assertEquals(List.of("node-1.node-2"), r.getMissingPartitionsList());
            assertEquals(10, r.getHitsCount(), "best 10 of the partitions that answered");
            for (ScoredChunk h : r.getHitsList()) assertFalse(c.ring.partitionOf(h.getChunkId()).id().equals("node-1.node-2"));
        }
        System.out.println("node-1 + node-2 down: partial=true missing=" + search().getMissingPartitionsList());
    }

    @Test
    void nothingAnsweredIsAnErrorNotAnEmptyList() {
        c.nodes.values().forEach(n -> n.crash());
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, this::search);
        assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
        System.out.println("all nodes down: " + e.getStatus());
    }
}
