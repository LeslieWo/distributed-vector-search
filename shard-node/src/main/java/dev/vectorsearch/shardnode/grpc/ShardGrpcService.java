package dev.vectorsearch.shardnode.grpc;

import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.DeleteRequest;
import dev.vectorsearch.proto.v1.DeleteResponse;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.HealthResponse;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardSearchResponse;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.proto.v1.UpsertResponse;
import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.proto.v1.ScanRequest;
import dev.vectorsearch.proto.v1.ScanResponse;
import dev.vectorsearch.shardnode.index.ChunkItem;
import dev.vectorsearch.shardnode.index.Hit;
import dev.vectorsearch.shardnode.store.PartitionStore;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** gRPC front of the partition store: validates requests, maps errors to gRPC status codes. */
public class ShardGrpcService extends ShardServiceGrpc.ShardServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ShardGrpcService.class);

    private final PartitionStore store;
    private final String nodeId;
    private final int dimension;
    private final int maxK;
    private final IntConsumer onUpserted;
    private volatile Runnable onExpired = () -> {};

    public ShardGrpcService(PartitionStore store) {
        this(store, n -> {});
    }

    /** @param onUpserted told how many chunks each successful Upsert applied (metrics hook) */
    public ShardGrpcService(PartitionStore store, IntConsumer onUpserted) {
        this.store = store;
        this.onUpserted = onUpserted;
        this.nodeId = store.properties().nodeId();
        this.dimension = store.properties().dimension();
        this.maxK = store.properties().maxK();
    }

    /** Metrics hook: called for each Search skipped because its deadline had passed. */
    public void setOnExpired(Runnable onExpired) {
        this.onExpired = onExpired;
    }

    @Override
    public void search(ShardSearchRequest req, StreamObserver<ShardSearchResponse> out) {
        // Deadline propagation (step 32): the caller's deadline travels with the request. If it already passed
        // (e.g. the request waited in our thread pool queue while we were overloaded) or the caller cancelled
        // (a hedge winner elsewhere), nobody will read the answer: skip the HNSW work.
        Context ctx = Context.current();
        if (ctx.isCancelled() || (ctx.getDeadline() != null && ctx.getDeadline().isExpired())) {
            onExpired.run();
            out.onError(Status.DEADLINE_EXCEEDED.withDescription("deadline passed before the search started").asRuntimeException());
            return;
        }
        handle(out, () -> {
            checkPartition(req.getPartition());
            if (req.getK() <= 0 || req.getK() > maxK) throw invalid("k must be in [1, " + maxK + "]");
            if (req.getEf() < 0) throw invalid("ef must be >= 0");
            float[] query = toArray(req.getVectorList(), "query");
            List<Hit> hits = store.search(req.getPartition(), query, req.getK(), req.getEf());
            ShardSearchResponse.Builder resp = ShardSearchResponse.newBuilder().setPartitionSize(store.size(req.getPartition()));
            for (Hit h : hits) {
                ScoredChunk.Builder c = ScoredChunk.newBuilder().setChunkId(h.id()).setScore(h.score());
                if (h.text() != null) c.setText(h.text());
                resp.addHits(c);
            }
            return resp.build();
        });
    }

    @Override
    public void upsert(UpsertRequest req, StreamObserver<UpsertResponse> out) {
        handle(out, () -> {
            // Validate the whole batch first so a bad entry rejects the batch without applying half of it.
            List<PartitionStore.Entry> entries = new ArrayList<>(req.getChunksCount());
            for (ChunkVector c : req.getChunksList()) {
                checkPartition(c.getPartition());
                if (c.getChunkId().isBlank()) throw invalid("blank chunk_id");
                entries.add(new PartitionStore.Entry(c.getChunkId(), c.getPartition(), toArray(c.getVectorList(), c.getChunkId()), c.getText()));
            }
            int n = store.upsert(entries, req.getOnlyIfAbsent());
            onUpserted.accept(n);
            return UpsertResponse.newBuilder().setUpserted(n).build();
        });
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<DeleteResponse> out) {
        handle(out, () -> {
            checkPartition(req.getPartition());
            return DeleteResponse.newBuilder().setDeleted(store.delete(req.getPartition(), req.getChunkIdsList())).build();
        });
    }

    @Override
    public void health(HealthRequest req, StreamObserver<HealthResponse> out) {
        handle(out, () -> HealthResponse.newBuilder()
                .setStatus(HealthResponse.Status.SERVING)
                .setNodeId(nodeId)
                .putAllPartitionSizes(store.sizes())
                .build());
    }

    /**
     * Streams a partition's chunks (all of them, or only those that move under a target ring), honouring gRPC
     * flow control: we only produce the next message when the transport says the client can take it, so a
     * large partition is never buffered in memory as a whole.
     */
    @Override
    public void scan(ScanRequest req, StreamObserver<ScanResponse> out) {
        HashRing target;
        List<ChunkItem> items;
        try {
            checkPartition(req.getPartition());
            target = req.getRingNodesCount() == 0 ? null
                    : HashRing.of(req.getRingNodesList(), req.getVnodes() > 0 ? req.getVnodes() : HashRing.DEFAULT_VNODES);
            items = store.items(req.getPartition());
        } catch (IllegalArgumentException e) {
            out.onError(invalid(e.getMessage()));
            return;
        } catch (StatusRuntimeException e) {
            out.onError(e);
            return;
        }
        int batch = req.getBatchSize() > 0 ? Math.min(req.getBatchSize(), 4096) : 256;
        ServerCallStreamObserver<ScanResponse> call = (ServerCallStreamObserver<ScanResponse>) out;
        Iterator<ChunkItem> it = items.iterator();
        AtomicBoolean finished = new AtomicBoolean();
        call.setOnCancelHandler(() -> finished.set(true));
        Runnable drain = () -> {
            synchronized (it) {
                while (!finished.get() && call.isReady()) {
                    ScanResponse.Builder msg = ScanResponse.newBuilder();
                    while (it.hasNext() && msg.getChunksCount() < batch) {
                        ChunkItem c = it.next();
                        String partition = target == null ? req.getPartition() : target.partitionOf(c.id()).id();
                        if (target != null && partition.equals(req.getPartition())) continue; // stays put
                        ChunkVector.Builder v = ChunkVector.newBuilder().setChunkId(c.id()).setPartition(partition)
                                .setText(c.text() == null ? "" : c.text());
                        for (float x : c.vector()) v.addVector(x);
                        msg.addChunks(v);
                    }
                    if (msg.getChunksCount() > 0) call.onNext(msg.build());
                    if (!it.hasNext()) {
                        finished.set(true);
                        call.onCompleted();
                    }
                }
            }
        };
        call.setOnReadyHandler(drain);
        drain.run();
    }

    private float[] toArray(List<Float> values, String what) {
        if (values.size() != dimension) throw invalid(what + ": expected " + dimension + " dims, got " + values.size());
        float[] v = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            float x = values.get(i);
            if (!Float.isFinite(x)) throw invalid(what + ": non-finite value at " + i);
            v[i] = x;
        }
        return v;
    }

    private static void checkPartition(String partition) {
        if (!PartitionStore.validPartitionName(partition)) throw invalid("bad partition name '" + partition + "'");
    }

    private static StatusRuntimeException invalid(String msg) {
        return Status.INVALID_ARGUMENT.withDescription(msg).asRuntimeException();
    }

    private interface Call<T> {
        T run() throws Exception;
    }

    private static <T> void handle(StreamObserver<T> out, Call<T> call) {
        T resp;
        try {
            resp = call.run();
        } catch (StatusRuntimeException e) {
            out.onError(e);
            return;
        } catch (Exception e) {
            log.error("shard rpc failed", e);
            out.onError(Status.INTERNAL.withDescription(e.toString()).withCause(e).asRuntimeException());
            return;
        }
        out.onNext(resp);
        out.onCompleted();
    }
}
