package dev.vectorsearch.common.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/** Step 24: heartbeats with TTL, a polling watcher, ring metadata; against a throwaway Redis. */
class RedisMembershipTest {
    GenericContainer<?> redis;
    String uri;

    @BeforeEach
    void up() {
        redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        redis.start();
        uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    }

    @AfterEach
    void down() {
        redis.stop();
    }

    static long waitUntil(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long t0 = System.nanoTime();
        while (!cond.getAsBoolean()) {
            if ((System.nanoTime() - t0) / 1_000_000 > timeoutMs) throw new AssertionError("condition not met in " + timeoutMs + " ms");
            Thread.sleep(10);
        }
        return (System.nanoTime() - t0) / 1_000_000;
    }

    @Test
    void ringIsInitialisedOnceAndNodesComeAndGoWithTheirHeartbeats() throws Exception {
        try (RingStore store = new RingStore(uri)) {
            assertTrue(store.initIfAbsent(List.of("node-2", "node-1")));
            assertFalse(store.initIfAbsent(List.of("node-9")), "an existing ring is never overwritten by init");
            assertEquals(new RedisKeys.RingSpec(1, List.of("node-1", "node-2")), store.ring());
        }
        AtomicInteger changes = new AtomicInteger();
        try (RedisClusterView watcher = new RedisClusterView(uri, Duration.ofMillis(50))) {
            watcher.onChange(v -> changes.incrementAndGet());
            assertEquals(List.of("node-1", "node-2"), watcher.view().ring().nodes());
            assertTrue(watcher.view().live().isEmpty());

            Heartbeater one = new Heartbeater(uri, "node-1", "localhost:9101", Duration.ofMillis(100), Duration.ofMillis(300));
            Heartbeater two = new Heartbeater(uri, "node-2", "localhost:9102", Duration.ofMillis(100), Duration.ofMillis(300));
            waitUntil(() -> watcher.view().live().size() == 2, 2000);
            assertEquals(Map.of("node-1", "localhost:9101", "node-2", "localhost:9102"), watcher.view().live());

            // crash: beats stop, nobody deletes the key; it must vanish by TTL (300 ms) + one poll
            two.stopBeating();
            long gone = waitUntil(() -> !watcher.view().isLive("node-2"), 2000);
            assertTrue(gone <= 300 + 50 + 200, "detected after " + gone + " ms");
            assertTrue(watcher.view().isLive("node-1"));

            // graceful leave: key deleted, visible after one poll
            one.close();
            assertTrue(waitUntil(() -> watcher.view().live().isEmpty(), 2000) < 250);
            two.close();
            assertTrue(changes.get() >= 3, "changes " + changes); // both up, node-2 gone, node-1 gone
        }
    }

    @Test
    void redisOutageKeepsTheLastViewInsteadOfDeclaringEveryoneDead() throws Exception {
        try (RingStore store = new RingStore(uri)) {
            store.initIfAbsent(List.of("node-1"));
        }
        Heartbeater hb = new Heartbeater(uri, "node-1", "localhost:9101", Duration.ofMillis(100), Duration.ofMillis(300));
        try (RedisClusterView watcher = new RedisClusterView(uri, Duration.ofMillis(50))) {
            waitUntil(() -> watcher.view().isLive("node-1"), 2000);
            redis.getDockerClient().pauseContainerCmd(redis.getContainerId()).exec();
            Thread.sleep(1500); // several failed polls (1 s command timeout)
            assertTrue(watcher.view().isLive("node-1"), "last known view is kept while Redis is unreachable");
            redis.getDockerClient().unpauseContainerCmd(redis.getContainerId()).exec();
            hb.close();
            waitUntil(() -> !watcher.view().isLive("node-1"), 5000);
        }
    }

    @Test
    void noRingYetMeansNoPlacement() {
        try (RedisClusterView watcher = new RedisClusterView(uri, Duration.ofMillis(50))) {
            assertFalse(watcher.view().hasRing());
            assertNull(watcher.view().pending());
        }
    }
}
