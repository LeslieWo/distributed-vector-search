package dev.vectorsearch.common.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Kafka topic layout (DESIGN.md "Kafka"). Topic auto-creation is disabled on the broker, so
 * services create these at startup via {@link TopicInitializer}.
 */
public final class Topics {
    private Topics() {}

    /** Chunks waiting to be embedded. Key = chunk id, value = {@code RawChunk} protobuf. */
    public static final String RAW_CHUNKS = "raw-chunks";
    /**
     * 12 = upper bound on parallel embed-workers in one consumer group (one partition is consumed by
     * at most one member). Divisible by 1, 2, 3, 4, 6, 12 so load spreads evenly for those counts.
     */
    public static final int RAW_CHUNKS_PARTITIONS = 12;

    /** Poison records that failed permanently; same key/value as raw-chunks plus error headers. */
    public static final String RAW_CHUNKS_DLQ = "raw-chunks.dlq";
    public static final int RAW_CHUNKS_DLQ_PARTITIONS = 3;

    public static List<NewTopic> all(short replicationFactor) {
        return List.of(
                new NewTopic(RAW_CHUNKS, RAW_CHUNKS_PARTITIONS, replicationFactor)
                        .configs(configs(replicationFactor, Duration.ofDays(7))),
                new NewTopic(RAW_CHUNKS_DLQ, RAW_CHUNKS_DLQ_PARTITIONS, replicationFactor)
                        .configs(configs(replicationFactor, Duration.ofDays(14))));
    }

    private static Map<String, String> configs(short rf, Duration retention) {
        return Map.of(
                // with acks=all, a write needs this many in-sync copies; 2 of 3 on AWS survives one broker loss
                "min.insync.replicas", String.valueOf(Math.min(2, (int) rf)),
                "retention.ms", String.valueOf(retention.toMillis()),
                "cleanup.policy", "delete");
    }
}
