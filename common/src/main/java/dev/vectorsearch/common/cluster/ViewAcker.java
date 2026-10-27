package dev.vectorsearch.common.cluster;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes which ring versions a writer (embed-worker) or reader (coordinator) is using:
 * {@code vs:views:<role>:<id>} = {@link #state}, TTL 3 s, refreshed every second.
 *
 * <p>A rebalance must not copy data until every writer writes to the pending ring too, and must not delete the
 * old copies until every reader reads from the new ring; it waits on these keys. A crashed process's key simply
 * expires, so it never blocks a rebalance for longer than the TTL.
 */
public final class ViewAcker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ViewAcker.class);
    public static final String WRITER = "writer";
    public static final String READER = "reader";

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> conn;
    private final String key;
    private final Supplier<String> state;
    private final long ttlMillis;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("view-ack").daemon().factory());

    /** @param state current versions, built with {@link #state(long, long)}; must reflect only views no older write/read still uses */
    public ViewAcker(String redisUri, String role, String id, Supplier<String> state, Duration interval, Duration ttl) {
        this.client = RedisClient.create(redisUri);
        this.client.setDefaultTimeout(interval);
        this.conn = client.connect();
        this.key = RedisKeys.VIEW_PREFIX + role + ":" + id;
        this.state = state;
        this.ttlMillis = ttl.toMillis();
        ack(); // registered before the owner starts writing/reading, so a rebalance can see it
        timer.scheduleAtFixedRate(this::ack, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static String state(long ringVersion, long pendingVersion) {
        return ringVersion + "|" + pendingVersion;
    }

    /** [ringVersion, pendingVersion]; pendingVersion = -1 when there is no pending ring. */
    public static long[] parse(String state) {
        String[] p = state.split("\\|");
        return new long[] {Long.parseLong(p[0]), Long.parseLong(p[1])};
    }

    private void ack() {
        try {
            conn.sync().set(key, state.get(), SetArgs.Builder.px(ttlMillis));
        } catch (RuntimeException e) {
            log.warn("view ack failed: {}", e.toString());
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
        try {
            conn.sync().del(key);
        } catch (RuntimeException ignored) {
            // expires by itself
        }
        conn.close();
        client.shutdown();
    }
}
