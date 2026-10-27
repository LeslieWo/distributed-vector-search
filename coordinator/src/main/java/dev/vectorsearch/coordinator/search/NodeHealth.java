package dev.vectorsearch.coordinator.search;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Fast-path failure detection on the data path (step 35). Membership (heartbeat TTL 3 s) is the slow path: a
 * killed node keeps its Redis key for up to 3 s. A gRPC UNAVAILABLE (connection refused/reset) tells us much
 * sooner, so the node is marked down at once and replica selection avoids it.
 *
 * <p>A mark is cleared when the node's heartbeat returns after having expired (it restarted), or after a
 * probation period, after which one request probes it again (covers a blip that never reached the TTL).
 */
public final class NodeHealth {
    private final long probationNanos;
    private final LongSupplier clock;
    private final Map<String, Long> downSince = new ConcurrentHashMap<>();

    public NodeHealth(long probationNanos, LongSupplier clock) {
        this.probationNanos = probationNanos;
        this.clock = clock;
    }

    public NodeHealth() {
        this(2_000_000_000L, System::nanoTime);
    }

    /** @return true if this call newly marked the node down */
    public boolean markDown(String node) {
        return downSince.put(node, clock.getAsLong()) == null;
    }

    public boolean isDown(String node) {
        Long since = downSince.get(node);
        if (since == null) return false;
        if (clock.getAsLong() - since < probationNanos) return true;
        downSince.remove(node, since); // probation over: let the next request probe it
        return false;
    }

    /** Membership saw these nodes come back (heartbeat present again after it had expired). */
    public void heartbeatReturned(Set<String> nodes) {
        nodes.forEach(downSince::remove);
    }
}
