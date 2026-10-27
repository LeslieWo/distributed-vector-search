package dev.vectorsearch.embedworker.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Collects items until either {@code maxItems} are buffered or the oldest buffered item has waited
 * {@code maxWaitMillis}. Count bound = throughput (full batches); time bound = latency cap when
 * traffic is light. Not thread-safe: used by the single consumer thread.
 */
public final class MicroBatcher<T> {
    private final int maxItems;
    private final long maxWaitMillis;
    private final LongSupplier clockMillis;
    private final List<T> items = new ArrayList<>();
    private long firstAddedAt;

    public MicroBatcher(int maxItems, long maxWaitMillis, LongSupplier clockMillis) {
        if (maxItems <= 0 || maxWaitMillis < 0) throw new IllegalArgumentException("maxItems > 0, maxWait >= 0");
        this.maxItems = maxItems;
        this.maxWaitMillis = maxWaitMillis;
        this.clockMillis = clockMillis;
    }

    public void add(T item) {
        if (items.isEmpty()) firstAddedAt = clockMillis.getAsLong();
        items.add(item);
    }

    public boolean isFull() {
        return items.size() >= maxItems;
    }

    /** Full, or non-empty and the oldest item has waited long enough. */
    public boolean ready() {
        return isFull() || (!items.isEmpty() && clockMillis.getAsLong() - firstAddedAt >= maxWaitMillis);
    }

    /** How long the caller may block waiting for more input before the time bound fires. */
    public long millisUntilDue(long idleWaitMillis) {
        if (items.isEmpty()) return idleWaitMillis;
        return Math.max(0, maxWaitMillis - (clockMillis.getAsLong() - firstAddedAt));
    }

    /** Removes and returns up to {@code maxItems} oldest items. */
    public List<T> drain() {
        int n = Math.min(maxItems, items.size());
        List<T> out = new ArrayList<>(items.subList(0, n));
        items.subList(0, n).clear();
        if (!items.isEmpty()) firstAddedAt = clockMillis.getAsLong();
        return out;
    }

    public int size() {
        return items.size();
    }

    public int maxItems() {
        return maxItems;
    }
}
