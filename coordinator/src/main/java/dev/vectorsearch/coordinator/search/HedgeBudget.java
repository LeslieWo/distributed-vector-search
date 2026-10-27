package dev.vectorsearch.coordinator.search;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Token bucket that caps hedges at {@code ratio} of primary requests (e.g. 0.1 = at most 10% extra load).
 * Every primary request deposits {@code ratio} tokens, a hedge spends one, the bucket holds at most
 * {@code burst} tokens.
 *
 * <p>Why a budget at all: when the whole cluster is slow because it is overloaded, every request looks like a
 * straggler; unbounded hedging would then add up to 100% more load exactly when there is no capacity, making
 * it slower still (a retry storm). The budget keeps the worst case at +ratio.
 */
public final class HedgeBudget {
    private static final long SCALE = 1000; // milli-tokens, so the ratio needs no floating point under contention
    private final long depositPerRequest;
    private final long capacity;
    private final AtomicLong tokens;

    public HedgeBudget(double ratio, int burst) {
        this.depositPerRequest = Math.round(ratio * SCALE);
        this.capacity = burst * SCALE;
        this.tokens = new AtomicLong(capacity);
    }

    public void onRequest() {
        tokens.getAndUpdate(t -> Math.min(capacity, t + depositPerRequest));
    }

    public boolean tryAcquire() {
        while (true) {
            long t = tokens.get();
            if (t < SCALE) return false;
            if (tokens.compareAndSet(t, t - SCALE)) return true;
        }
    }
}
