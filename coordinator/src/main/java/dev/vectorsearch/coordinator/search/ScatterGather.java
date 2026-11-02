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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Scatter: one Search per partition of the ring, each sent to one live holder, all in parallel.
 * Gather: wait for all of them (bounded by the query deadline) and merge into the global top-k.
 *
 * <p>Every chunk is in exactly one partition (step 22), so asking each partition once covers
 * every vector exactly once. Per partition, a hedged request goes to the other holder when the first one has
 * not answered within the hedge delay (step 34); the first answer wins and the other call is cancelled.
 */
public final class ScatterGather implements ShardSearcher {
    private final ClusterViewSource cluster;
    private final ChannelPool channels;
    private final ReplicaSelector selector;
    private final NodeStats stats;
    private final NodeHealth health;
    private final int efSearch;
    private final long reserveMicros;
    private final long shardTimeoutMicros;
    private final HedgePolicy hedgePolicy; // null = hedging off
    private final HedgeBudget hedgeBudget;
    private volatile boolean cancelHedgeLosers = true;
    /** A timed-out attempt is retried on the other replica only if at least this much query time is left. */
    static final long MIN_RETRY_MILLIS = 10;
    private final ScheduledExecutorService timers =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("hedge-timer").daemon().factory());
    private final AtomicReference<ClusterView> lastView = new AtomicReference<>();
    final Counter hedgesSent;
    final Counter hedgesWon;
    final Counter hedgesDenied;
    final Counter partitionRequests;
    final Counter retries;
    final Counter markedDown;

    /**
     * @param responseReserve shards are given the query deadline minus this, so that when they time out the
     *                        coordinator still has time to merge and answer with partial results
     * @param shardTimeout    cap on one partition request even when the query deadline is far away
     * @param hedgePolicy     when to hedge; null disables hedging
     */
    public ScatterGather(ClusterViewSource cluster, ChannelPool channels, ReplicaSelector selector, NodeStats stats, NodeHealth health,
                         int efSearch, Duration responseReserve, Duration shardTimeout, HedgePolicy hedgePolicy, HedgeBudget hedgeBudget,
                         MeterRegistry registry) {
        this.cluster = cluster;
        this.channels = channels;
        this.selector = selector;
        this.stats = stats;
        this.health = health;
        this.efSearch = efSearch;
        this.reserveMicros = responseReserve.toNanos() / 1000;
        this.shardTimeoutMicros = shardTimeout.toNanos() / 1000;
        this.hedgePolicy = hedgePolicy;
        this.hedgeBudget = hedgeBudget;
        this.hedgesSent = Counter.builder("vs.coord.hedge.sent").description("hedged partition requests sent").register(registry);
        this.hedgesWon = Counter.builder("vs.coord.hedge.won").description("hedged requests that answered first").register(registry);
        this.hedgesDenied = Counter.builder("vs.coord.hedge.denied").description("hedges skipped because the budget was empty").register(registry);
        this.partitionRequests = Counter.builder("vs.coord.partition.requests").description("first-choice partition requests").register(registry);
        this.retries = Counter.builder("vs.coord.retries").description("partition requests retried on the other replica after a failure").register(registry);
        this.markedDown = Counter.builder("vs.coord.node.marked.down").description("nodes marked down after UNAVAILABLE").register(registry);
    }

    /** Without hedging (tests of the earlier steps). */
    public ScatterGather(ClusterViewSource cluster, ChannelPool channels, ReplicaSelector selector, NodeStats stats, int efSearch,
                         Duration responseReserve, Duration shardTimeout) {
        this(cluster, channels, selector, stats, new NodeHealth(), efSearch, responseReserve, shardTimeout, null, null,
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
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
        // Each attempt gets min(query deadline - reserve, now + shard timeout) when it is sent (shardDeadline); the
        // gather waits until the query deadline - reserve, so an attempt that hits the per-attempt cap can still be
        // retried on the other replica while the query has time (step 43).
        Deadline deadline = Deadline.after(Math.max(0, queryDeadline.timeRemaining(TimeUnit.MICROSECONDS) - reserveMicros), TimeUnit.MICROSECONDS);
        ClusterView view = cluster.view();
        if (!view.hasRing()) throw Status.UNAVAILABLE.withDescription("no ring in cluster metadata").asRuntimeException();
        syncChannels(view);
        ShardSearchRequest.Builder base = ShardSearchRequest.newBuilder().setK(k).setEf(efSearch);
        for (float x : query) base.addVector(x);

        Map<Partition, PartitionCall> calls = new LinkedHashMap<>();
        for (Partition p : view.ring().partitions()) {
            PartitionCall call = new PartitionCall(p, view, base.setPartition(p.id()).build(), queryDeadline, deadline);
            calls.put(p, call);
            call.start();
        }
        try {
            CompletableFuture.allOf(calls.values().stream().map(c -> c.result).toArray(CompletableFuture[]::new))
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
        calls.forEach((p, call) -> {
            PartitionResult r = call.result.getNow(null);
            if (r == null) {
                call.result.cancel(true); // cancels every attempt still in flight
                missing.add(p.id());
            } else if (r.error() != null) {
                missing.add(p.id());
            } else {
                results.add(r);
            }
        });
        return new Result(TopKMerger.merge(results.stream().map(PartitionResult::hits).toList(), k), missing, calls.size());
    }

    /** One partition's request: first choice, optional hedge, first success wins. */
    private final class PartitionCall {
        final Partition partition;
        final ClusterView view;
        final ShardSearchRequest request;
        final Deadline queryDeadline;
        final Deadline deadline; // query deadline - reserve: no attempt may run past it
        final CompletableFuture<PartitionResult> result = new CompletableFuture<>();
        private final Map<String, ListenableFuture<ShardSearchResponse>> attempts = new LinkedHashMap<>(); // guarded by this
        private final Set<String> failed = new HashSet<>();
        private final List<String> errors = new ArrayList<>();
        private boolean hedgeTimerPending; // guarded by this

        PartitionCall(Partition partition, ClusterView view, ShardSearchRequest request, Deadline queryDeadline, Deadline deadline) {
            this.partition = partition;
            this.view = view;
            this.request = request;
            this.queryDeadline = queryDeadline;
            this.deadline = deadline;
            // gave up or timed out: stop every attempt. Winner found: stop the loser only if cancelHedgeLosers (step 41)
            result.whenComplete((r, e) -> {
                if (e != null || cancelHedgeLosers) cancelAll();
            });
        }

        /** Live holders not marked down; if every live holder is marked down, try them anyway. */
        private List<String> candidates() {
            List<String> live = ReplicaSelector.liveHolders(partition, view);
            List<String> up = live.stream().filter(n -> !health.isDown(n)).toList();
            return up.isEmpty() ? live : up;
        }

        void start() {
            List<String> live = candidates();
            if (live.isEmpty()) {
                result.complete(new PartitionResult(partition, List.of(), "no live holder"));
                return;
            }
            partitionRequests.increment();
            if (hedgeBudget != null) hedgeBudget.onRequest();
            send(selector.choose(partition, live), false);
            if (hedgePolicy != null && live.size() > 1) {
                long delay = hedgePolicy.delayNanos(partition);
                if (delay < deadline.timeRemaining(TimeUnit.NANOSECONDS)) {
                    synchronized (this) {
                        hedgeTimerPending = true;
                    }
                    timers.schedule(this::hedge, delay, TimeUnit.NANOSECONDS);
                }
            }
        }

        private void hedge() {
            String other;
            synchronized (this) {
                hedgeTimerPending = false;
                if (result.isDone()) return;
                other = untried();
            }
            if (other != null && hedgeBudget.tryAcquire()) {
                hedgesSent.increment();
                send(other, true);
                return;
            }
            if (other != null) hedgesDenied.increment();
            completeIfAllFailed(); // the first attempt may already have failed while we waited
        }

        /** A live holder this call has not sent to yet (null if none). Caller holds the lock. */
        private String untried() {
            return candidates().stream().filter(n -> !attempts.containsKey(n)).findFirst().orElse(null);
        }

        private void completeIfAllFailed() {
            boolean allFailed;
            synchronized (this) {
                allFailed = !hedgeTimerPending && failed.containsAll(attempts.keySet());
            }
            if (allFailed) result.complete(new PartitionResult(partition, List.of(), String.join("; ", errors)));
        }

        private void send(String node, boolean isHedge) {
            long t0 = System.nanoTime();
            stats.started(node);
            ListenableFuture<ShardSearchResponse> call = ShardServiceGrpc.newFutureStub(channels.get(view.address(node)))
                    .withDeadline(shardDeadline(queryDeadline)).search(request);
            synchronized (this) {
                attempts.put(node, call);
                if (result.isDone()) call.cancel(true); // lost a race with completion
            }
            Futures.addCallback(call, new FutureCallback<>() {
                @Override
                public void onSuccess(ShardSearchResponse r) {
                    long latency = System.nanoTime() - t0;
                    stats.finished(node, latency);
                    if (hedgePolicy != null) hedgePolicy.record(partition, latency);
                    if (result.complete(new PartitionResult(partition, r.getHitsList(), null)) && isHedge) hedgesWon.increment();
                }

                @Override
                public void onFailure(Throwable t) {
                    long elapsed = System.nanoTime() - t0;
                    if (call.isCancelled()) { // we cancelled it ourselves (the other attempt won): it was at least this slow
                        stats.finished(node, elapsed);
                        return;
                    }
                    // A real failure counts as slow (at least the shard timeout): a node refusing connections answers
                    // in microseconds and would otherwise look like the fastest replica to p2c.
                    stats.finished(node, Math.max(elapsed, shardTimeoutMicros * 1000));
                    Status status = Status.fromThrowable(t);
                    if (status.getCode() == Status.Code.UNAVAILABLE && health.markDown(node)) markedDown.increment();
                    String retryOn;
                    synchronized (PartitionCall.this) {
                        failed.add(node);
                        errors.add(node + ": " + status);
                        // retry right away on the other replica (not budgeted: the failed attempt cost the cluster nothing)
                        // DEADLINE_EXCEEDED: retry only if the query still has time (the attempt hit the per-attempt
                        // shard timeout, not the query deadline)
                        boolean noTime = status.getCode() == Status.Code.DEADLINE_EXCEEDED
                                && deadline.timeRemaining(TimeUnit.MILLISECONDS) < MIN_RETRY_MILLIS;
                        retryOn = result.isDone() || noTime ? null : untried();
                    }
                    if (retryOn != null) {
                        retries.increment();
                        send(retryOn, false);
                        return;
                    }
                    completeIfAllFailed();
                }
            }, MoreExecutors.directExecutor());
        }

        private synchronized void cancelAll() {
            for (ListenableFuture<ShardSearchResponse> a : attempts.values()) if (!a.isDone()) a.cancel(true);
        }
    }

    /**
     * true (step 34): cancel the slower attempt as soon as one answers, saving the shard the work. false (step 41):
     * let it finish and ignore its hits, so NodeStats sees its real latency and keeps it counted in flight until it
     * answers. Cancelling hides a stalled replica from p2c: the loser leaves in-flight at once and is recorded at
     * the hedge time instead of its true latency.
     */
    public void setCancelHedgeLosers(boolean cancel) {
        this.cancelHedgeLosers = cancel;
    }

    /**
     * On a membership change: fresh channels for nodes that come back (ChannelPool.retainOnly, step 26)
     * and clear their down marks (their heartbeat returned after it had expired, i.e. they restarted).
     */
    private void syncChannels(ClusterView view) {
        ClusterView prev = lastView.get();
        if (prev == view || !lastView.compareAndSet(prev, view)) return;
        channels.retainOnly(new HashSet<>(view.live().values()));
        if (prev != null) {
            Set<String> returned = new HashSet<>(view.live().keySet());
            returned.removeAll(prev.live().keySet());
            health.heartbeatReturned(returned);
        }
    }
}
