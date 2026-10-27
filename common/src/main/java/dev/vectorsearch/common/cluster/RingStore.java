package dev.vectorsearch.common.cluster;

import dev.vectorsearch.common.ring.HashRing;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Operator-side access to the ring metadata in Redis (init, inspect; rebalance steps in step 27). */
public final class RingStore implements AutoCloseable {
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> conn;

    public RingStore(String redisUri) {
        this.client = RedisClient.create(redisUri);
        this.conn = client.connect();
    }

    RedisCommands<String, String> redis() {
        return conn.sync();
    }

    /** Creates ring version 1 if no ring exists yet. @return false if a ring was already there (left untouched) */
    public boolean initIfAbsent(List<String> nodes) {
        HashRing.of(nodes); // validates node ids
        String v = RedisKeys.encodeRing(1, new java.util.TreeSet<>(nodes).stream().toList());
        return "OK".equals(redis().set(RedisKeys.RING, v, SetArgs.Builder.nx()));
    }

    public RedisKeys.RingSpec ring() {
        return RedisKeys.decodeRing(redis().get(RedisKeys.RING));
    }

    public RedisKeys.RingSpec pending() {
        return RedisKeys.decodeRing(redis().get(RedisKeys.RING_PENDING));
    }

    /**
     * Starts a rebalance towards {@code target}: writes {@code vs:ring:pending} = (ring version + 1, target).
     * Re-running with the same target resumes an interrupted rebalance; a different target is refused.
     * @return the pending version
     */
    public long beginRebalance(List<String> target) {
        List<String> sorted = new java.util.TreeSet<>(target).stream().toList();
        HashRing.of(sorted); // validates
        RedisKeys.RingSpec ring = ring();
        if (ring == null) throw new IllegalStateException("no ring yet: run init first");
        RedisKeys.RingSpec pending = pending();
        if (pending != null) {
            if (pending.nodes().equals(sorted)) return pending.version(); // resume
            throw new IllegalStateException("another rebalance is pending: " + pending + " (abort it first)");
        }
        if (ring.nodes().equals(sorted)) throw new IllegalStateException("ring is already " + sorted);
        long version = ring.version() + 1;
        redis().set(RedisKeys.RING_PENDING, RedisKeys.encodeRing(version, sorted));
        return version;
    }

    /** Makes the pending ring the ring, atomically (MULTI/EXEC). */
    public void commitRebalance() {
        RedisKeys.RingSpec pending = pending();
        if (pending == null) throw new IllegalStateException("nothing pending");
        RedisCommands<String, String> r = redis();
        r.multi();
        r.set(RedisKeys.RING, RedisKeys.encodeRing(pending.version(), pending.nodes()));
        r.del(RedisKeys.RING_PENDING);
        if (r.exec().wasDiscarded()) throw new IllegalStateException("ring commit discarded");
    }

    /** Drops the pending ring. The current ring was never changed, so readers are unaffected. */
    public void abortRebalance() {
        redis().del(RedisKeys.RING_PENDING);
    }

    /** id -> state of every live writer or reader ({@link ViewAcker#WRITER} / {@link ViewAcker#READER}). */
    public Map<String, String> views(String role) {
        Map<String, String> out = new TreeMap<>();
        String prefix = RedisKeys.VIEW_PREFIX + role + ":";
        for (String k : redis().keys(prefix + "*")) {
            String v = redis().get(k);
            if (v != null) out.put(k.substring(prefix.length()), v);
        }
        return out;
    }

    /** node -> address of every node with a current heartbeat. */
    public Map<String, String> liveNodes() {
        Map<String, String> out = new TreeMap<>();
        for (String k : redis().keys(RedisKeys.NODE_PREFIX + "*")) {
            String v = redis().get(k);
            if (v != null) out.put(k.substring(RedisKeys.NODE_PREFIX.length()), v);
        }
        return out;
    }

    @Override
    public void close() {
        conn.close();
        client.shutdown();
    }
}
