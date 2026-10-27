package dev.vectorsearch.common.cluster;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps {@code vs:nodes:<nodeId>} alive: SET with a TTL, refreshed more often than the TTL. If the process
 * dies (kill -9, machine gone, network partition from Redis) the refreshes stop and the key expires by
 * itself, so readers drop the node within one TTL without anyone having to notice the crash.
 */
public final class Heartbeater implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Heartbeater.class);

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> conn;
    private final String key;
    private final String address;
    private final Duration ttl;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("heartbeat").daemon().factory());
    private volatile boolean failing;

    public Heartbeater(String redisUri, String nodeId, String address, Duration interval, Duration ttl) {
        if (ttl.compareTo(interval.multipliedBy(2)) < 0) throw new IllegalArgumentException("ttl must be >= 2 x interval");
        this.client = RedisClient.create(redisUri);
        this.client.setDefaultTimeout(interval); // a slow Redis must not stack up beats
        this.conn = client.connect();
        this.key = RedisKeys.node(nodeId);
        this.address = address;
        this.ttl = ttl;
        beat(); // register right away (the caller starts us once the gRPC port is listening)
        timer.scheduleAtFixedRate(this::beat, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("heartbeating {} = {} every {} ms, ttl {} ms", key, address, interval.toMillis(), ttl.toMillis());
    }

    private void beat() {
        try {
            conn.sync().set(key, address, SetArgs.Builder.px(ttl.toMillis()));
            if (failing) log.info("heartbeat to Redis recovered");
            failing = false;
        } catch (RuntimeException e) {
            if (!failing) log.warn("heartbeat failed (will keep trying): {}", e.toString());
            failing = true;
        }
    }

    /** Stops beating without deleting the key: what a crash looks like to everyone else (for tests). */
    public void stopBeating() {
        timer.shutdownNow();
    }

    /** Graceful leave: delete the key so readers stop routing here now instead of after the TTL. */
    @Override
    public void close() {
        timer.shutdownNow();
        try {
            conn.sync().del(key);
        } catch (RuntimeException e) {
            log.warn("could not delete {} on shutdown, it expires in {} ms: {}", key, ttl.toMillis(), e.toString());
        }
        conn.close();
        client.shutdown();
    }
}
