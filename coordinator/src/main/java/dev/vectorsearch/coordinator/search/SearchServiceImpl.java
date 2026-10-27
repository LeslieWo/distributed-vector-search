package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.coordinator.CoordinatorProperties;
import dev.vectorsearch.coordinator.cache.QueryCache;
import dev.vectorsearch.embedding.Embedder;
import dev.vectorsearch.proto.v1.SearchRequest;
import dev.vectorsearch.proto.v1.SearchResponse;
import dev.vectorsearch.proto.v1.SearchServiceGrpc;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Public query API: text -> (cache) -> embedding -> scatter-gather over all partitions -> top-k with text.
 */
public class SearchServiceImpl extends SearchServiceGrpc.SearchServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(SearchServiceImpl.class);
    static final int MAX_QUERY_CHARS = 2000;

    private final CoordinatorProperties props;
    private final Embedder embedder;
    private final ShardSearcher searcher;
    private final QueryCache cache; // null = disabled
    private final MeterRegistry registry;
    private final Timer embedTimer;
    private final Counter partialCounter;

    public SearchServiceImpl(CoordinatorProperties props, Embedder embedder, ShardSearcher searcher, QueryCache cache, MeterRegistry registry) {
        this.props = props;
        this.embedder = embedder;
        this.searcher = searcher;
        this.cache = cache;
        this.registry = registry;
        this.embedTimer = Timer.builder("vs.coord.embed").description("query embedding time").publishPercentileHistogram().register(registry);
        this.partialCounter = Counter.builder("vs.coord.partial").description("responses with at least one missing partition").register(registry);
    }

    @Override
    public void search(SearchRequest req, StreamObserver<SearchResponse> out) {
        long t0 = System.nanoTime();
        String text = req.getText();
        if (text.isBlank() || text.length() > MAX_QUERY_CHARS) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("text must be 1.." + MAX_QUERY_CHARS + " characters").asRuntimeException());
            return;
        }
        int k = req.getK() == 0 ? props.defaultK() : req.getK();
        if (k < 1 || k > props.maxK()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("k must be in [1, " + props.maxK() + "]").asRuntimeException());
            return;
        }
        // The client's deadline if it sent one, otherwise ours (step 32).
        Deadline clientDeadline = Context.current().getDeadline();
        Deadline deadline = clientDeadline != null ? clientDeadline : Deadline.after(props.defaultDeadline().toMillis(), TimeUnit.MILLISECONDS);

        if (cache != null) {
            Optional<SearchResponse> hit = cache.get(text, k);
            if (hit.isPresent()) {
                record(t0, "hit", false);
                out.onNext(hit.get());
                out.onCompleted();
                return;
            }
        }
        float[] query;
        try {
            long e0 = System.nanoTime();
            query = embedder.embed(text);
            embedTimer.record(System.nanoTime() - e0, TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            log.error("query embedding failed", e);
            out.onError(Status.INTERNAL.withDescription("embedding failed: " + e).asRuntimeException());
            return;
        }
        ShardSearcher.Result r = searcher.search(query, k, deadline);
        SearchResponse resp = SearchResponse.newBuilder().addAllHits(r.hits()).setPartial(!r.missingPartitions().isEmpty()).build();
        if (resp.getPartial()) partialCounter.increment();
        if (cache != null) cache.put(text, k, resp); // skips partial responses
        record(t0, "miss", resp.getPartial());
        out.onNext(resp);
        out.onCompleted();
    }

    private void record(long t0, String cacheResult, boolean partial) {
        Timer.builder("vs.coord.search").description("end-to-end query latency inside the coordinator")
                .tag("cache", cacheResult).tag("partial", String.valueOf(partial))
                .publishPercentileHistogram().register(registry)
                .record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
    }
}
