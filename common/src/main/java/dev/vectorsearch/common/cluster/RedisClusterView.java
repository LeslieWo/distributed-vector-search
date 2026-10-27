package dev.vectorsearch.common.cluster;

import dev.vectorsearch.common.ring.HashRing;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches cluster metadata in Redis by polling (default every 500 ms): live nodes from the heartbeat keys,
 * the ring and the pending ring. {@link #view()} is a lock-free read of the latest snapshot.
 *
 * <p>If Redis is unreachable the last known view is kept: losing the metadata store must not make every
 * node look dead. Node failures are still detected on the data path (gRPC errors, step 35).
 */
public final class RedisClusterView implements ClusterViewSource {
    private static final Logger log = LoggerFactory.getLogger(RedisClusterView.class);

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> conn;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("cluster-view").daemon().factory());
    private final List<Consumer<ClusterView>> listeners = new CopyOnWriteArrayList<>();
    private volatile ClusterView view = new ClusterView(-1, null, null, Map.of());
    private volatile boolean failing;

    public RedisClusterView(String redisUri, Duration pollInterval) {
        this.client = RedisClient.create(redisUri);
        this.client.setDefaultTimeout(Duration.ofSeconds(1));
        this.conn = client.connect();
        refresh(); // have a view before the first caller asks
        timer.scheduleWithFixedDelay(this::refresh, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public ClusterView view() {
        return view;
    }

    /** Called (on the watcher thread) whenever the view changes. */
    public void onChange(Consumer<ClusterView> listener) {
        listeners.add(listener);
    }

    void refresh() {
        try {
            RedisCommands<String, String> redis = conn.sync();
            List<String> keys = new ArrayList<>();
            ScanCursor cursor = ScanCursor.INITIAL;
            do {
                KeyScanCursor<String> page = redis.scan(cursor, ScanArgs.Builder.matches(RedisKeys.NODE_PREFIX + "*").limit(1000));
                keys.addAll(page.getKeys());
                cursor = page;
            } while (!cursor.isFinished());
            Map<String, String> live = new TreeMap<>();
            if (!keys.isEmpty()) {
                for (KeyValue<String, String> kv : redis.mget(keys.toArray(String[]::new))) {
                    // a key can expire between SCAN and MGET: that node just died, skip it
                    if (kv.hasValue()) live.put(kv.getKey().substring(RedisKeys.NODE_PREFIX.length()), kv.getValue());
                }
            }
            RedisKeys.RingSpec ring = RedisKeys.decodeRing(redis.get(RedisKeys.RING));
            RedisKeys.RingSpec pending = RedisKeys.decodeRing(redis.get(RedisKeys.RING_PENDING));
            ClusterView old = view;
            HashRing r = ring == null ? null : sameNodes(old.ring(), ring.nodes()) ? old.ring() : HashRing.of(ring.nodes());
            HashRing p = pending == null ? null : sameNodes(old.pending(), pending.nodes()) ? old.pending() : HashRing.of(pending.nodes());
            ClusterView next = new ClusterView(ring == null ? -1 : ring.version(), r, p, live);
            if (failing) log.info("Redis reachable again");
            failing = false;
            if (!next.equals(old)) {
                view = next;
                log.info("cluster view: ring v{} {} pending {} live {}", next.ringVersion(), r == null ? "-" : r.nodes(),
                        p == null ? "-" : p.nodes(), live.keySet());
                for (Consumer<ClusterView> l : listeners) {
                    try {
                        l.accept(next);
                    } catch (RuntimeException e) {
                        log.warn("cluster view listener failed", e);
                    }
                }
            }
        } catch (RuntimeException e) {
            if (!failing) log.warn("cannot read cluster metadata from Redis, keeping the last view: {}", e.toString());
            failing = true;
        }
    }

    private static boolean sameNodes(HashRing ring, List<String> nodes) {
        return ring != null && Objects.equals(ring.nodes(), new java.util.TreeSet<>(nodes).stream().toList());
    }

    @Override
    public void close() {
        timer.shutdownNow();
        conn.close();
        client.shutdown();
    }
}
