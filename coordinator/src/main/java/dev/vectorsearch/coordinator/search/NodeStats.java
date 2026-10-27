package dev.vectorsearch.coordinator.search;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Per shard node, as seen from this coordinator: requests in flight and a "peak EWMA" of latency.
 *
 * <p>Peak EWMA (as in Finagle / Linkerd): a sample above the average replaces it at once (react to a node
 * getting slow immediately), a sample below is blended in with a weight that depends on how much time passed
 * (time constant {@code tau}). When reading, the value decays towards 0 with the time since the last sample, so
 * a node we stopped sending to because it was slow is tried again after a while instead of being starved
 * forever; if it is still slow, the next sample pushes it right back up.
 */
public final class NodeStats {
    private final double tauNanos;
    private final LongSupplier clock;
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();

    public NodeStats(double tauMillis, LongSupplier nanoClock) {
        this.tauNanos = tauMillis * 1e6;
        this.clock = nanoClock;
    }

    public NodeStats() {
        this(1000, System::nanoTime);
    }

    static final class Node {
        final AtomicInteger inFlight = new AtomicInteger();
        double ewmaNanos; // guarded by this
        long lastNanos;
    }

    private Node node(String id) {
        return nodes.computeIfAbsent(id, k -> {
            Node n = new Node();
            n.lastNanos = clock.getAsLong();
            return n;
        });
    }

    public void started(String node) {
        node(node).inFlight.incrementAndGet();
    }

    /** @param latencyNanos time from send to answer (or to failure; failures are recorded as slow, see step 35) */
    public void finished(String node, long latencyNanos) {
        Node n = node(node);
        n.inFlight.decrementAndGet();
        synchronized (n) {
            long now = clock.getAsLong();
            if (latencyNanos > n.ewmaNanos) {
                n.ewmaNanos = latencyNanos; // peak: slowness counts immediately
            } else {
                double w = Math.exp(-(now - n.lastNanos) / tauNanos);
                n.ewmaNanos = n.ewmaNanos * w + latencyNanos * (1 - w);
            }
            n.lastNanos = now;
        }
    }

    public int inFlight(String node) {
        return node(node).inFlight.get();
    }

    /** Latency estimate now: the EWMA decayed by the time since the last sample. */
    public double latencyNanos(String node) {
        Node n = node(node);
        synchronized (n) {
            return n.ewmaNanos * Math.exp(-(clock.getAsLong() - n.lastNanos) / tauNanos);
        }
    }

    /** Lower is better: expected wait if we add one more request = latency x (in flight + 1). */
    public double cost(String node) {
        return (latencyNanos(node) + 1) * (inFlight(node) + 1);
    }
}
