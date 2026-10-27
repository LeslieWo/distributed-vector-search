package dev.vectorsearch.coordinator.cache;

import dev.vectorsearch.embedding.MiniLmEmbedder;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.SearchResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;

/**
 * Cache hit path vs the work it saves: {@code ./gradlew -q :coordinator:cacheBench} (needs local Redis).
 * Hit = Redis GET + protobuf parse of a k=10 response with passage texts.
 */
public final class CacheBench {
    public static void main(String[] args) throws Exception {
        String passage = "x".repeat(380); // MS MARCO passages average ~350-400 characters
        SearchResponse.Builder b = SearchResponse.newBuilder();
        for (int i = 0; i < 10; i++) b.addHits(ScoredChunk.newBuilder().setChunkId("100000" + i + "#0").setScore(0.7f).setText(passage));
        SearchResponse resp = b.build();
        try (QueryCache cache = new QueryCache("redis://localhost:6379", Duration.ofSeconds(60), Duration.ofMillis(50))) {
            cache.put("bench query", 10, resp);
            Thread.sleep(100);
            for (int i = 0; i < 2000; i++) cache.get("bench query", 10); // warm-up (JIT, connection)
            long[] ns = new long[5000];
            for (int i = 0; i < ns.length; i++) {
                long t0 = System.nanoTime();
                if (cache.get("bench query", 10).isEmpty()) throw new IllegalStateException("miss");
                ns[i] = System.nanoTime() - t0;
            }
            System.out.printf("cache hit (GET + parse, %d bytes): %s%n", resp.getSerializedSize(), stats(ns));
        }
        try (MiniLmEmbedder embedder = new MiniLmEmbedder(Path.of("data/models/all-MiniLM-L6-v2"))) {
            String[] queries = {"what is the manhattan project", "how long does it take to boil an egg", "cost of endless pools/swim spa",
                    "symptoms of a heart attack in women", "what county is phoenix az in"};
            for (int i = 0; i < 100; i++) embedder.embed(queries[i % queries.length]);
            long[] ns = new long[500];
            for (int i = 0; i < ns.length; i++) {
                long t0 = System.nanoTime();
                embedder.embed(queries[i % queries.length]);
                ns[i] = System.nanoTime() - t0;
            }
            System.out.printf("query embedding (what a hit skips, before any shard call): %s%n", stats(ns));
        }
    }

    static String stats(long[] ns) {
        long[] s = ns.clone();
        Arrays.sort(s);
        return String.format("p50=%.3fms p99=%.3fms max=%.3fms (n=%d)", s[s.length / 2] / 1e6, s[(int) (s.length * 0.99)] / 1e6, s[s.length - 1] / 1e6, s.length);
    }
}
