package dev.vectorsearch.shardnode.metrics;

import dev.vectorsearch.shardnode.store.PersistentPartitionStore;
import dev.vectorsearch.shardnode.store.PartitionStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Index-level metrics: vectors per partition, upserted chunks, WAL size, snapshot duration. */
public final class ShardMetrics implements AutoCloseable {
    private final PersistentPartitionStore store;
    private final MultiGauge indexSize;
    private final Counter upsertedChunks;
    private final ScheduledExecutorService refresher =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("metrics-refresh").daemon().factory());

    public ShardMetrics(MeterRegistry registry, PersistentPartitionStore store) {
        this.store = store;
        this.indexSize = MultiGauge.builder("vs.shard.index.size")
                .description("vectors stored per partition").baseUnit("vectors").register(registry);
        this.upsertedChunks = Counter.builder("vs.shard.upserted.chunks")
                .description("chunks applied by Upsert (re-deliveries included)").register(registry);
        FunctionCounter.builder("vs.shard.upsert.duplicates", store, PartitionStore::duplicateUpserts)
                .description("upserts skipped because the same chunk with the same vector was already stored").register(registry);
        FunctionCounter.builder("vs.shard.upsert.replaced", store, PartitionStore::replacedVectors)
                .description("upserts that replaced a vector (each leaves a soft-deleted HNSW node)").register(registry);
        Gauge.builder("vs.shard.wal.size", store, PersistentPartitionStore::walSizeBytes).baseUnit("bytes").register(registry);
        Timer snapshots = Timer.builder("vs.shard.snapshot").description("time to write all partition snapshots").register(registry);
        store.setSnapshotListener((millis, partitions) -> snapshots.record(Duration.ofMillis(millis)));
        // Partitions appear dynamically (first upsert, or restore), so re-publish the rows periodically.
        refresh();
        refresher.scheduleWithFixedDelay(this::refresh, 2, 2, TimeUnit.SECONDS);
    }

    public void recordUpserted(int n) {
        upsertedChunks.increment(n);
    }

    public void refresh() {
        indexSize.register(store.sizes().entrySet().stream()
                .map(e -> MultiGauge.Row.of(Tags.of("partition", e.getKey()), e.getValue()))
                .toList(), true);
    }

    @Override
    public void close() {
        refresher.shutdownNow();
    }
}
