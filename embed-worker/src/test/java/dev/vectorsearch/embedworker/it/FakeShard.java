package dev.vectorsearch.embedworker.it;

import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.proto.v1.UpsertResponse;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/** Real gRPC server on a random port that records upserts per chunk id and can be told to fail. */
public final class FakeShard implements AutoCloseable {
    public final Map<String, AtomicInteger> upsertsPerChunk = new ConcurrentHashMap<>();
    public final AtomicLong totalUpserts = new AtomicLong();
    public final AtomicLong rejectedCalls = new AtomicLong();
    /** while > 0, each call fails with UNAVAILABLE and decrements */
    public final AtomicInteger failNextCalls = new AtomicInteger();
    public volatile boolean down;
    /** chunks matching this are rejected with INVALID_ARGUMENT (the whole call, like the real shard-node) */
    public volatile Predicate<ChunkVector> invalid = c -> false;
    private final Server server;

    public FakeShard() throws Exception {
        server = NettyServerBuilder.forPort(0).addService(new ShardServiceGrpc.ShardServiceImplBase() {
            @Override
            public void upsert(UpsertRequest req, StreamObserver<UpsertResponse> out) {
                if (down || failNextCalls.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                    rejectedCalls.incrementAndGet();
                    out.onError(Status.UNAVAILABLE.withDescription("fake outage").asRuntimeException());
                    return;
                }
                for (ChunkVector c : req.getChunksList()) {
                    if (invalid.test(c)) {
                        rejectedCalls.incrementAndGet();
                        out.onError(Status.INVALID_ARGUMENT.withDescription("bad chunk " + c.getChunkId()).asRuntimeException());
                        return;
                    }
                }
                for (ChunkVector c : req.getChunksList()) {
                    upsertsPerChunk.computeIfAbsent(c.getChunkId(), k -> new AtomicInteger()).incrementAndGet();
                    totalUpserts.incrementAndGet();
                }
                out.onNext(UpsertResponse.newBuilder().setUpserted(req.getChunksCount()).build());
                out.onCompleted();
            }
        }).build().start();
    }

    public String target() {
        return "localhost:" + server.getPort();
    }

    public Set<String> chunkIds() {
        return upsertsPerChunk.keySet();
    }

    @Override
    public void close() {
        server.shutdownNow();
    }
}
