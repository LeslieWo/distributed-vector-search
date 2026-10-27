package dev.vectorsearch.embedworker.shard;

import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Phase 2 stand-in for the ring router: every chunk goes to one node, into one partition. */
public final class SingleShardWriter implements ShardWriter {
    private static final Set<Status.Code> RETRIABLE = Set.of(
            Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED, Status.Code.RESOURCE_EXHAUSTED,
            Status.Code.ABORTED, Status.Code.INTERNAL, Status.Code.UNKNOWN, Status.Code.CANCELLED);

    private final ManagedChannel channel;
    private final String partition;
    private final long deadlineMillis;

    public SingleShardWriter(ManagedChannel channel, String partition, long deadlineMillis) {
        this.channel = channel;
        this.partition = partition;
        this.deadlineMillis = deadlineMillis;
    }

    @Override
    public void write(List<EmbeddedChunk> chunks) {
        UpsertRequest.Builder req = UpsertRequest.newBuilder();
        for (EmbeddedChunk c : chunks) {
            ChunkVector.Builder v = ChunkVector.newBuilder().setChunkId(c.chunkId()).setPartition(partition).setText(c.text());
            for (float x : c.vector()) v.addVector(x);
            req.addChunks(v);
        }
        try {
            ShardServiceGrpc.newBlockingStub(channel).withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS).upsert(req.build());
        } catch (StatusRuntimeException e) {
            // INTERNAL counts as retriable: e.g. a failed WAL fsync on the node is transient.
            throw new ShardWriteException("upsert of " + chunks.size() + " chunks failed: " + e.getStatus(),
                    RETRIABLE.contains(e.getStatus().getCode()), e);
        }
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}
