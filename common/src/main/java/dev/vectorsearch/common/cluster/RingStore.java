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
