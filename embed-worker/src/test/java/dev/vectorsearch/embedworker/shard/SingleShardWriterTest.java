package dev.vectorsearch.embedworker.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.proto.v1.UpsertResponse;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SingleShardWriterTest {
    final List<UpsertRequest> received = new ArrayList<>();
    final AtomicReference<Status> failWith = new AtomicReference<>();
    Server server;
    SingleShardWriter writer;

    void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(new ShardServiceGrpc.ShardServiceImplBase() {
            @Override
            public void upsert(UpsertRequest req, StreamObserver<UpsertResponse> out) {
                if (failWith.get() != null) {
                    out.onError(failWith.get().asRuntimeException());
                    return;
                }
                received.add(req);
                out.onNext(UpsertResponse.newBuilder().setUpserted(req.getChunksCount()).build());
                out.onCompleted();
            }
        }).build().start();
        writer = new SingleShardWriter(InProcessChannelBuilder.forName(name).directExecutor().build(), "node-1", 1000);
    }

    @AfterEach
    void stop() {
        writer.close();
        server.shutdownNow();
    }

    @Test
    void sendsEveryChunkToTheConfiguredPartition() throws Exception {
        start();
        writer.write(List.of(new EmbeddedChunk("a#0", new float[] {1, 0}, "A"), new EmbeddedChunk("b#0", new float[] {0, 1}, "B")));
        assertEquals(1, received.size());
        assertEquals("node-1", received.get(0).getChunks(1).getPartition());
        assertEquals("B", received.get(0).getChunks(1).getText());
    }

    @Test
    void classifiesFailuresAsRetriableOrNot() throws Exception {
        start();
        List<EmbeddedChunk> one = List.of(new EmbeddedChunk("a#0", new float[] {1, 0}, "A"));
        failWith.set(Status.UNAVAILABLE);
        assertTrue(assertThrows(ShardWriteException.class, () -> writer.write(one)).retriable());
        failWith.set(Status.INVALID_ARGUMENT);
        assertFalse(assertThrows(ShardWriteException.class, () -> writer.write(one)).retriable());
    }
}
