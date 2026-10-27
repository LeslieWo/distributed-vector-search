package dev.vectorsearch.coordinator.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.SearchResponse;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

class QueryCacheTest {
    GenericContainer<?> redis;
    QueryCache cache;

    static final SearchResponse RESP = SearchResponse.newBuilder()
            .addHits(ScoredChunk.newBuilder().setChunkId("1#0").setScore(0.9f).setText("hello")).build();

    @BeforeEach
    void up() {
        redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        redis.start();
        cache = new QueryCache("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379), Duration.ofMillis(500), Duration.ofMillis(200));
    }

    @AfterEach
    void down() {
        cache.close();
        redis.stop();
    }

    static Optional<SearchResponse> eventually(QueryCache c, String text, int k) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Optional<SearchResponse> r = c.get(text, k);
            if (r.isPresent()) return r;
            Thread.sleep(5);
        }
        return Optional.empty();
    }

    @Test
    void missThenHitAndEquivalentSpellingsShareAnEntry() throws Exception {
        assertTrue(cache.get("What is RAM?", 10).isEmpty());
        cache.put("What is RAM?", 10, RESP);
        assertEquals(RESP, eventually(cache, "What is RAM?", 10).orElseThrow());
        assertEquals(RESP, cache.get("  what   is ram?\n", 10).orElseThrow()); // same embedding -> same key
        assertTrue(cache.get("What is RAM?", 5).isEmpty(), "k is part of the key");
        assertNotEquals(QueryCache.key("what is ram", 10), QueryCache.key("what is ram?", 10)); // punctuation is a token
    }

    @Test
    void entriesExpireAfterTtlAndPartialResultsAreNeverCached() throws Exception {
        cache.put("q", 10, RESP);
        assertTrue(eventually(cache, "q", 10).isPresent());
        Thread.sleep(700);
        assertTrue(cache.get("q", 10).isEmpty(), "TTL 500 ms");
        cache.put("p", 10, RESP.toBuilder().setPartial(true).build());
        Thread.sleep(50);
        assertTrue(cache.get("p", 10).isEmpty());
        assertEquals(1, cache.skippedPartial.get());
    }

    @Test
    void unreachableRedisIsAFastMissNotAnError() {
        redis.getDockerClient().pauseContainerCmd(redis.getContainerId()).exec();
        try {
            long t0 = System.nanoTime();
            assertTrue(cache.get("q", 10).isEmpty());
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 1000, "bounded by the 200 ms command timeout, took " + ms);
            cache.put("q", 10, RESP); // must not throw
            assertEquals(1, cache.errors.get());
        } finally {
            redis.getDockerClient().unpauseContainerCmd(redis.getContainerId()).exec();
        }
    }
}
