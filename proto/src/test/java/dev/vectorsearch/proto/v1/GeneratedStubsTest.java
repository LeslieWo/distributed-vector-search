package dev.vectorsearch.proto.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Proves the generated stubs compile and can round-trip a call over an in-process channel. */
class GeneratedStubsTest {

    @Test
    void searchRoundTripsThroughInProcessServer() throws Exception {
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new ShardServiceGrpc.ShardServiceImplBase() {
                    @Override
                    public void search(ShardSearchRequest req, StreamObserver<ShardSearchResponse> out) {
                        out.onNext(ShardSearchResponse.newBuilder()
                                .addHits(ScoredChunk.newBuilder().setChunkId("d1#0").setScore(req.getVector(0)))
                                .setPartitionSize(1)
                                .build());
                        out.onCompleted();
                    }
                })
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            ShardSearchResponse resp = ShardServiceGrpc.newBlockingStub(channel).search(
                    ShardSearchRequest.newBuilder().setPartition("node-1").addAllVector(List.of(0.5f, 0.1f)).setK(1).build());
            assertEquals("d1#0", resp.getHits(0).getChunkId());
            assertEquals(0.5f, resp.getHits(0).getScore());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
