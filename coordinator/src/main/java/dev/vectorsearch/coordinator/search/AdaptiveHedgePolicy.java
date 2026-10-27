package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.common.ring.Partition;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hedge delay = rolling p95 of the partition's recent attempt latencies (last 256), never below a floor.
 * Until 32 samples exist the configured initial delay is used.
 *
 * <p>Why p95: by definition only ~5% of requests take longer, so hedging at p95 sends ~5% extra requests while
 * cutting off the slowest 5%, which is exactly what dominates p99 of a fan-out query. Why per partition:
 * partitions differ in size and therefore in latency. Why a floor: on a fast LAN p95 can be well under a
 * millisecond, where the difference is noise and a hedge mostly doubles work.
 */
public final class AdaptiveHedgePolicy implements HedgePolicy {
    static final int WINDOW = 256;
    static final int MIN_SAMPLES = 32;

    private final long floorNanos;
    private final long initialNanos;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public AdaptiveHedgePolicy(long floorNanos, long initialNanos) {
        this.floorNanos = floorNanos;
        this.initialNanos = initialNanos;
    }

    static final class Window {
        final long[] samples = new long[WINDOW];
        long count;
        volatile long p95 = -1;
    }

    @Override
    public long delayNanos(Partition partition) {
        Window w = windows.get(partition.id());
        long p95 = w == null ? -1 : w.p95;
        return Math.max(floorNanos, p95 < 0 ? initialNanos : p95);
    }

    @Override
    public void record(Partition partition, long latencyNanos) {
        Window w = windows.computeIfAbsent(partition.id(), k -> new Window());
        synchronized (w) {
            w.samples[(int) (w.count % WINDOW)] = latencyNanos;
            w.count++;
            // recompute every 16 samples: a 256-element sort is ~microseconds, not worth doing per request
            if (w.count >= MIN_SAMPLES && w.count % 16 == 0) {
                long[] copy = Arrays.copyOf(w.samples, (int) Math.min(w.count, WINDOW));
                Arrays.sort(copy);
                w.p95 = copy[(int) (copy.length * 0.95)];
            }
        }
    }
}
