package dev.vectorsearch.embedworker.shard;

import com.google.common.util.concurrent.ListenableFuture;
import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.ClusterView;
import dev.vectorsearch.common.cluster.ClusterViewSource;
import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.proto.v1.UpsertResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Routes each chunk to both holders of its partition on the consistent-hash ring (and, during a rebalance,
 * also to its holders under the pending ring), one Upsert per node, sent in parallel.
 *
 * <p>Write ack policy = ALL (step 26): {@link #write} returns only when every target node acked.
 * Any failure throws, the consumer does not commit the Kafka offset, and the whole batch is retried later.
 * Re-sending to the nodes that already acked is harmless because Upsert is idempotent by chunk id.
 *
 * <p>Stateless apart from the channel cache, so any number of embed-worker processes can run it at once.
 */
public final class RingShardWriter implements ShardWriter {
    static final Set<Status.Code> RETRIABLE = Set.of(
            Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED, Status.Code.RESOURCE_EXHAUSTED,
            Status.Code.ABORTED, Status.Code.INTERNAL, Status.Code.UNKNOWN, Status.Code.CANCELLED);

    private final ClusterViewSource cluster;
    private final ChannelPool channels;
    private final long deadlineMillis;
    private ClusterView lastView; // written only by the consumer thread

    public RingShardWriter(ClusterViewSource cluster, ChannelPool channels, long deadlineMillis) {
        this.cluster = cluster;
        this.channels = channels;
        this.deadlineMillis = deadlineMillis;
    }

    /**
     * The ring versions this writer is on, for the rebalance barrier ({@code ViewAcker}). Taking the writer's
     * lock means no write that started with an older view is still in flight, and every later write calls
     * {@code cluster.view()} again, so it uses at least these versions.
     */
    public synchronized String ackState() {
        ClusterView v = cluster.view();
        return dev.vectorsearch.common.cluster.ViewAcker.state(v.ringVersion(), v.pendingVersion());
    }

    @Override
    public synchronized void write(List<EmbeddedChunk> chunks) {
        ClusterView view = cluster.view();
        if (view != lastView) {
            channels.retainOnly(new java.util.HashSet<>(view.live().values())); // see ChannelPool: fresh channel on return
            lastView = view;
        }
        if (!view.hasRing()) throw new ShardWriteException("no ring in cluster metadata yet (run ClusterAdmin init)", true, null);
        Map<String, UpsertRequest.Builder> perNode = new LinkedHashMap<>();
        for (EmbeddedChunk c : chunks) {
            ChunkVector.Builder base = ChunkVector.newBuilder().setChunkId(c.chunkId()).setText(c.text());
            for (float x : c.vector()) base.addVector(x);
            for (ClusterView.Target t : view.writeTargets(c.chunkId())) {
                perNode.computeIfAbsent(t.node(), n -> UpsertRequest.newBuilder()).addChunks(base.clone().setPartition(t.partition()));
            }
        }
        // ALL: a target without a heartbeat cannot ack, so don't bother the others now; the batch is retried
        // as a whole once it is back (or once an operator removed it from the ring).
        Set<String> down = new TreeSet<>();
        for (String node : perNode.keySet()) if (!view.isLive(node)) down.add(node);
        if (!down.isEmpty()) {
            throw new ShardWriteException("no live address for " + down + " (" + chunks.size() + " chunks wait, ack=ALL)", true, null);
        }

        Map<String, ListenableFuture<UpsertResponse>> calls = new LinkedHashMap<>();
        perNode.forEach((node, req) -> calls.put(node, ShardServiceGrpc.newFutureStub(channels.get(view.address(node)))
                .withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS).upsert(req.build())));

        List<String> failures = new ArrayList<>();
        boolean allPermanent = true;
        StatusRuntimeException first = null;
        for (Map.Entry<String, ListenableFuture<UpsertResponse>> e : calls.entrySet()) {
            try {
                e.getValue().get();
            } catch (ExecutionException ex) {
                Status s = Status.fromThrowable(ex.getCause());
                failures.add(e.getKey() + ": " + s);
                allPermanent &= !RETRIABLE.contains(s.getCode());
                if (first == null) first = s.asRuntimeException();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new ShardWriteException("interrupted while writing", true, ex);
            }
        }
        if (!failures.isEmpty()) {
            // Retriable if any node failed for infrastructure reasons: the same request may then succeed.
            throw new ShardWriteException("upsert of " + chunks.size() + " chunks failed on " + failures, !allPermanent, first);
        }
    }

    @Override
    public void close() {
        channels.close(); // the cluster view is its own bean with its own lifecycle
    }
}
