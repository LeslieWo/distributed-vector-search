package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.coordinator.CoordinatorProperties;
import dev.vectorsearch.embedding.HashingEmbedder;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.SearchRequest;
import dev.vectorsearch.proto.v1.SearchResponse;
import dev.vectorsearch.proto.v1.SearchServiceGrpc;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Step 29: validation, embedding with the shared model code, deadline handed to the searcher. */
class SearchServiceImplTest {
    final HashingEmbedder embedder = new HashingEmbedder(384);
    final AtomicReference<float[]> seenQuery = new AtomicReference<>();
    final AtomicReference<Deadline> seenDeadline = new AtomicReference<>();
    Server server;
    ManagedChannel channel;
    SearchServiceGrpc.SearchServiceBlockingStub stub;

    @BeforeEach
    void up() throws Exception {
        ShardSearcher fake = (query, k, deadline) -> {
            seenQuery.set(query);
            seenDeadline.set(deadline);
            return new ShardSearcher.Result(List.of(ScoredChunk.newBuilder().setChunkId("1#0").setScore(0.5f).setText("t").build()), List.of());
        };
        CoordinatorProperties props = new CoordinatorProperties(0, 4, null, null, "hashing", null, 10, 100, 0, null, null, null, null, false, null, null);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new SearchServiceImpl(props, embedder, fake, null, new SimpleMeterRegistry())).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = SearchServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void down() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void embedsTheQueryTextAndReturnsTheSearcherHits() {
        SearchResponse r = stub.search(SearchRequest.newBuilder().setText("what is a vector index").setK(5).build());
        assertEquals("1#0", r.getHits(0).getChunkId());
        assertArrayEquals(embedder.embed("what is a vector index"), seenQuery.get(), 0f);
        // no client deadline -> the coordinator's default (1 s)
        long left = seenDeadline.get().timeRemaining(TimeUnit.MILLISECONDS);
        assertTrue(left > 500 && left <= 1000, "default deadline, " + left + " ms left");
    }

    @Test
    void theClientsDeadlineIsPassedOn() {
        stub.withDeadlineAfter(300, TimeUnit.MILLISECONDS).search(SearchRequest.newBuilder().setText("q").build());
        assertTrue(seenDeadline.get().timeRemaining(TimeUnit.MILLISECONDS) <= 300);
    }

    @Test
    void rejectsBadRequests() {
        for (SearchRequest bad : List.of(SearchRequest.newBuilder().setText(" ").build(),
                SearchRequest.newBuilder().setText("q").setK(101).build(),
                SearchRequest.newBuilder().setText("q").setK(-1).build(),
                SearchRequest.newBuilder().setText("x".repeat(2001)).build())) {
            StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> stub.search(bad));
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        }
    }
}
