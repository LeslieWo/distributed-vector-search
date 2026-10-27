package dev.vectorsearch.coordinator.search;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.ClusterView;
import dev.vectorsearch.common.cluster.ClusterViewSource;
import dev.vectorsearch.common.ring.Partition;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardSearchResponse;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import io.grpc.Deadline;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Scatter: one Search per partition of the ring, each sent to one live holder, all in parallel.
 * Gather: wait for all of them (bounded by the query deadline) and merge into the global top-k.
 *
 * <p>Every chunk is in exactly one partition (BUILDLOG step 22), so asking each partition once covers
 * every vector exactly once.
 */
public final class ScatterGather implements ShardSearcher {
    private final ClusterViewSource cluster;
    private final ChannelPool channels;
    private final ReplicaSelector selector;
    private final NodeStats stats;
    private final int efSearch;
    private final long reserveMicros;
    private final long shardTimeoutMicros;
    private final AtomicReference<ClusterView> lastView = new AtomicReference<>();

    /**
     * @param responseReserve shards are given the query deadline minus this, so that when they time out the
     *                        coordinator still has time to merge and answer with partial results
     * @param shardTimeout    cap on one partition request even when the query deadline is far away
     */
    public ScatterGather(ClusterViewSource cluster, ChannelPool channels, ReplicaSelector selector, NodeStats stats, int efSearch,
                         Duration responseReserve, Duration shardTimeout) {
        this.cluster = cluster;
        this.channels = channels;
        this.selector = selector;
        this.stats = stats;
        this.efSearch = efSearch;
        this.reserveMicros = responseReserve.toNanos() / 1000;
        this.shardTimeoutMicros = shardTimeout.toNanos() / 1000;
    }

    /** Deadline for the partition requests: min(query deadline - reserve, now + shard timeout). */
    Deadline shardDeadline(Deadline queryDeadline) {
        long budget = Math.min(queryDeadline.timeRemaining(TimeUnit.MICROSECONDS) - reserveMicros, shardTimeoutMicros);
        return Deadline.after(Math.max(0, budget), TimeUnit.MICROSECONDS);
    }

    /** Outcome of one partition: its hits, or why it is missing. */
    record PartitionResult(Partition partition, List<ScoredChunk> hits, String error) {}

    @Override
    public Result search(float[] query, int k, Deadline queryDeadline) {
        Deadline deadline = shardDeadline(queryDeadline); // every partition request shares this budget
        ClusterView view = cluster.view();
        if (!view.hasRing()) throw Status.UNAVAILABLE.withDescription("no ring in cluster metadata").asRuntimeException();
        syncChannels(view);
        ShardSearchRequest.Builder base = ShardSearchRequest.newBuilder().setK(k).setEf(efSearch);
        for (float x : query) base.addVector(x);

        Map<Partition, CompletableFuture<PartitionResult>> calls = new LinkedHashMap<>();
        for (Partition p : view.ring().partitions()) calls.put(p, searchPartition(p, view, base.setPartition(p.id()).build(), deadline));

        try {
            CompletableFuture.allOf(calls.values().toArray(CompletableFuture[]::new))
                    .get(Math.max(0, deadline.timeRemaining(TimeUnit.MICROSECONDS)), TimeUnit.MICROSECONDS);
        } catch (TimeoutException e) {
            // fall through: unfinished partitions are reported missing below
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("partition futures never complete exceptionally", e);
        }
        List<PartitionResult> results = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        calls.forEach((p, f) -> {
            PartitionResult r = f.getNow(null);
            if (r == null) {
                f.cancel(true);
                missing.add(p.id());
            } else if (r.error() != null) {
                missing.add(p.id());
            } else {
                results.add(r);
            }
        });
        return new Result(TopKMerger.merge(results.stream().map(PartitionResult::hits).toList(), k), missing);
    }

    private CompletableFuture<PartitionResult> searchPartition(Partition p, ClusterView view, ShardSearchRequest req, Deadline deadline) {
        List<String> live = ReplicaSelector.liveHolders(p, view);
        if (live.isEmpty()) return CompletableFuture.completedFuture(new PartitionResult(p, List.of(), "no live holder"));
        String node = selector.choose(p, live);
        long t0 = System.nanoTime();
        stats.started(node);
        ListenableFuture<ShardSearchResponse> call = ShardServiceGrpc.newFutureStub(channels.get(view.address(node)))
                .withDeadline(deadline).search(req);
        CompletableFuture<PartitionResult> out = new CompletableFuture<>();
        Futures.addCallback(call, new FutureCallback<>() {
            @Override
            public void onSuccess(ShardSearchResponse r) {
                stats.finished(node, System.nanoTime() - t0);
                out.complete(new PartitionResult(p, r.getHitsList(), null));
            }

            @Override
            public void onFailure(Throwable t) {
                stats.finished(node, System.nanoTime() - t0); // a timeout records the full wait: the node looks slow
                out.complete(new PartitionResult(p, List.of(), node + ": " + Status.fromThrowable(t)));
            }
        }, MoreExecutors.directExecutor());
        out.whenComplete((r, e) -> {
            if (out.isCancelled()) call.cancel(true); // gave up on this partition: cancel the RPC on the shard too
        });
        return out;
    }

    /** Fresh channel for a node that comes back (see ChannelPool.retainOnly, BUILDLOG step 26). */
    private void syncChannels(ClusterView view) {
        ClusterView prev = lastView.get();
        if (prev != view && lastView.compareAndSet(prev, view)) channels.retainOnly(new HashSet<>(view.live().values()));
    }
}
