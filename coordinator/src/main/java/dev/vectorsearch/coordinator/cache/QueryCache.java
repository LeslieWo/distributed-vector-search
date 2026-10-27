package dev.vectorsearch.coordinator.cache;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.vectorsearch.proto.v1.SearchResponse;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis cache of complete search responses: key = sha256(normalized query text + k), TTL 60 s.
 *
 * <p>A hit skips the query embedding (the coordinator's most expensive step) and the whole scatter-gather.
 * The cache is strictly best effort: Redis errors and timeouts count as misses and never fail a query.
 */
public final class QueryCache implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(QueryCache.class);
    private static final String PREFIX = "vs:qc:v1:"; // bump v1 when the value format or embedding model changes

    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> conn;
    private final long ttlMillis;
    public final AtomicLong hits = new AtomicLong();
    public final AtomicLong misses = new AtomicLong();
    public final AtomicLong errors = new AtomicLong();
    public final AtomicLong skippedPartial = new AtomicLong();

    public QueryCache(String redisUri, Duration ttl, Duration timeout) {
        this.client = RedisClient.create(redisUri);
        this.client.setDefaultTimeout(timeout); // a slow Redis must not make queries slow: give up, treat as miss
        this.conn = client.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
        this.ttlMillis = ttl.toMillis();
    }

    /**
     * Only transformations the embedding model also applies (all-MiniLM-L6-v2's tokenizer lowercases and splits
     * on whitespace), so two queries share a key only if they get the same embedding.
     */
    public static String normalize(String text) {
        return text.strip().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }

    public static String key(String text, int k) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest((normalize(text) + "\u0000" + k).getBytes(StandardCharsets.UTF_8));
            return PREFIX + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public Optional<SearchResponse> get(String text, int k) {
        try {
            byte[] v = conn.sync().get(key(text, k));
            if (v == null) {
                misses.incrementAndGet();
                return Optional.empty();
            }
            hits.incrementAndGet();
            return Optional.of(SearchResponse.parseFrom(v));
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            errors.incrementAndGet();
            log.debug("query cache get failed, treating as miss: {}", e.toString());
            return Optional.empty();
        }
    }

    /** Fire-and-forget SET with TTL. Partial results are not cached: one bad moment must not stick for 60 s. */
    public void put(String text, int k, SearchResponse response) {
        if (response.getPartial()) {
            skippedPartial.incrementAndGet();
            return;
        }
        try {
            conn.async().set(key(text, k), response.toByteArray(), SetArgs.Builder.px(ttlMillis))
                    .exceptionally(e -> {
                        errors.incrementAndGet();
                        return null;
                    });
        } catch (RuntimeException e) {
            errors.incrementAndGet();
        }
    }

    @Override
    public void close() {
        conn.close();
        client.shutdown();
    }
}
