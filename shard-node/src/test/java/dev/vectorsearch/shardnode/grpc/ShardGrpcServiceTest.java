package dev.vectorsearch.shardnode.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.DeleteRequest;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.HealthResponse;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardSearchResponse;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.store.PartitionStore;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShardGrpcServiceTest {
    static final int DIM = 4;
    Server server;
    ManagedChannel channel;
    ShardServiceGrpc.ShardServiceBlockingStub stub;

    static ShardNodeProperties props(int dim) {
        return ShardNodeProperties.forTest("node-1", dim, null, null, 0);
    }

    @BeforeEach
    void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new ShardGrpcService(new PartitionStore(props(DIM)))).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = ShardServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    static ChunkVector chunk(String id, String partition, float... v) {
        List<Float> list = new ArrayList<>();
        for (float x : v) list.add(x);
        return ChunkVector.newBuilder().setChunkId(id).setPartition(partition).addAllVector(list).setText("text of " + id).build();
    }

    static ShardSearchRequest query(String partition, int k, float... v) {
        List<Float> list = new ArrayList<>();
        for (float x : v) list.add(x);
        return ShardSearchRequest.newBuilder().setPartition(partition).setK(k).addAllVector(list).build();
    }

    @Test
    void upsertThenSearchPerPartition() {
        stub.upsert(UpsertRequest.newBuilder()
                .addChunks(chunk("a#0", "node-1", 1, 0, 0, 0))
                .addChunks(chunk("b#0", "node-1", 0, 1, 0, 0))
                .addChunks(chunk("c#0", "node-3", 1, 0, 0, 0)) // replica partition on the same node
                .build());

        ShardSearchResponse r = stub.search(query("node-1", 2, 1, 0, 0, 0));
        assertEquals(List.of("a#0", "b#0"), r.getHitsList().stream().map(h -> h.getChunkId()).toList());
        assertEquals(1f, r.getHits(0).getScore(), 1e-6);
        assertEquals("text of a#0", r.getHits(0).getText());
        assertEquals(2, r.getPartitionSize());

        // partitions are isolated: node-3 only has c#0
        assertEquals("c#0", stub.search(query("node-3", 5, 1, 0, 0, 0)).getHits(0).getChunkId());
        assertEquals(0, stub.search(query("node-2", 5, 1, 0, 0, 0)).getHitsCount());

        HealthResponse h = stub.health(HealthRequest.getDefaultInstance());
        assertEquals(HealthResponse.Status.SERVING, h.getStatus());
        assertEquals(Map.of("node-1", 2L, "node-3", 1L), h.getPartitionSizesMap());
    }

    @Test
    void sameUpsertBatchThreeTimesChangesNothing() {
        UpsertRequest batch = UpsertRequest.newBuilder()
                .addChunks(chunk("a#0", "node-1", 1, 0, 0, 0)).addChunks(chunk("b#0", "node-1", 0, 1, 0, 0))
                .addChunks(chunk("a#0", "node-1", 1, 0, 0, 0)) // duplicate inside one batch too
                .build();
        stub.upsert(batch);
        ShardSearchResponse first = stub.search(query("node-1", 5, 1, 0, 0, 0));
        stub.upsert(batch);
        stub.upsert(batch);
        assertEquals(Map.of("node-1", 2L), stub.health(HealthRequest.getDefaultInstance()).getPartitionSizesMap());
        assertEquals(first, stub.search(query("node-1", 5, 1, 0, 0, 0)));
    }

    @Test
    void deleteRemovesChunks() {
        stub.upsert(UpsertRequest.newBuilder().addChunks(chunk("a#0", "p", 1, 0, 0, 0)).addChunks(chunk("b#0", "p", 0, 1, 0, 0)).build());
        assertEquals(1, stub.delete(DeleteRequest.newBuilder().setPartition("p").addChunkIds("a#0").addChunkIds("zzz").build()).getDeleted());
        assertEquals(List.of("b#0"), stub.search(query("p", 5, 1, 0, 0, 0)).getHitsList().stream().map(h -> h.getChunkId()).toList());
    }

    @Test
    void invalidRequestsAreRejectedWithInvalidArgument() {
        assertInvalid(() -> stub.search(query("node-1", 0, 1, 0, 0, 0)));        // k = 0
        assertInvalid(() -> stub.search(query("node-1", 5, 1, 0, 0)));           // wrong dimension
        assertInvalid(() -> stub.search(query("../etc", 5, 1, 0, 0, 0)));        // bad partition name
        assertInvalid(() -> stub.upsert(UpsertRequest.newBuilder()
                .addChunks(chunk("ok#0", "p", 1, 0, 0, 0))
                .addChunks(chunk("bad#0", "p", Float.NaN, 0, 0, 0)).build()));
        // the whole batch was rejected, including the valid first entry
        assertEquals(0, stub.search(query("p", 5, 1, 0, 0, 0)).getHitsCount());
    }

    private static void assertInvalid(Runnable call) {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, call::run);
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void onlyIfAbsentNeverOverwritesAnExistingChunk() {
        stub.upsert(UpsertRequest.newBuilder().addChunks(chunk("a#0", "p", 1, 0, 0, 0)).build());
        // a rebalance copy arriving after a newer live write must lose
        stub.upsert(UpsertRequest.newBuilder().setOnlyIfAbsent(true)
                .addChunks(chunk("a#0", "p", 0, 1, 0, 0)).addChunks(chunk("b#0", "p", 0, 0, 1, 0)).build());
        assertEquals("a#0", stub.search(query("p", 1, 1, 0, 0, 0)).getHits(0).getChunkId());
        assertEquals(1f, stub.search(query("p", 1, 1, 0, 0, 0)).getHits(0).getScore(), 1e-6); // old vector kept
        assertEquals(2, stub.search(query("p", 1, 1, 0, 0, 0)).getPartitionSize()); // b#0 was absent, so added
    }

    @Test
    void scanStreamsAllChunksOrOnlyThoseThatMoveUnderATargetRing() {
        dev.vectorsearch.common.ring.HashRing three = dev.vectorsearch.common.ring.HashRing.of(List.of("node-1", "node-2", "node-3"));
        dev.vectorsearch.common.ring.HashRing four = dev.vectorsearch.common.ring.HashRing.of(List.of("node-1", "node-2", "node-3", "node-4"));
        UpsertRequest.Builder req = UpsertRequest.newBuilder();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            String id = i + "#0";
            if (!three.partitionOf(id).id().equals("node-1.node-2")) continue;
            ids.add(id);
            req.addChunks(chunk(id, "node-1.node-2", 1, i, 0, 0));
        }
        stub.upsert(req.build());

        List<ChunkVector> all = new ArrayList<>();
        stub.scan(dev.vectorsearch.proto.v1.ScanRequest.newBuilder().setPartition("node-1.node-2").setBatchSize(50).build())
                .forEachRemaining(m -> all.addAll(m.getChunksList()));
        assertEquals(ids.size(), all.size());
        assertEquals(new java.util.HashSet<>(ids), all.stream().map(ChunkVector::getChunkId).collect(java.util.stream.Collectors.toSet()));
        assertEquals(DIM, all.get(0).getVectorCount());

        List<ChunkVector> moved = new ArrayList<>();
        stub.scan(dev.vectorsearch.proto.v1.ScanRequest.newBuilder().setPartition("node-1.node-2").addAllRingNodes(four.nodes()).build())
                .forEachRemaining(m -> moved.addAll(m.getChunksList()));
        long expected = ids.stream().filter(id -> !four.partitionOf(id).id().equals("node-1.node-2")).count();
        assertEquals(expected, moved.size());
        assertTrue(expected > 0 && expected < ids.size());
        for (ChunkVector c : moved) assertEquals(four.partitionOf(c.getChunkId()).id(), c.getPartition());

        assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
                () -> stub.scan(dev.vectorsearch.proto.v1.ScanRequest.newBuilder().setPartition("p").addRingNodes("bad.id").build()).hasNext())
                .getStatus().getCode());
    }
}
