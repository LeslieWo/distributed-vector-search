package dev.vectorsearch.embedworker;

import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "worker")
public record WorkerProperties(
        String bootstrapServers,
        String groupId,
        short replicationFactor,
        int batchSize,
        Duration batchMaxWait,
        int maxPollRecords,
        Duration maxPollInterval,
        String embedder,
        Path modelDir,
        String shardTarget,
        String shardPartition,
        Duration shardDeadline,
        Duration retryBackoffInitial,
        Duration retryBackoffMax,
        int maxAttempts) {

    public WorkerProperties {
        if (bootstrapServers == null || bootstrapServers.isBlank()) bootstrapServers = "localhost:9092";
        if (groupId == null || groupId.isBlank()) groupId = "embed-worker";
        if (replicationFactor <= 0) replicationFactor = 1;
        if (batchSize <= 0) batchSize = 64;
        if (batchMaxWait == null) batchMaxWait = Duration.ofMillis(50);
        if (maxPollRecords <= 0) maxPollRecords = 256;
        if (maxPollInterval == null) maxPollInterval = Duration.ofMinutes(5);
        if (embedder == null || embedder.isBlank()) embedder = "onnx";
        if (modelDir == null) modelDir = Path.of("data/models/all-MiniLM-L6-v2");
        if (shardTarget == null || shardTarget.isBlank()) shardTarget = "localhost:9101";
        if (shardPartition == null || shardPartition.isBlank()) shardPartition = "node-1";
        if (shardDeadline == null) shardDeadline = Duration.ofSeconds(10);
        if (retryBackoffInitial == null) retryBackoffInitial = Duration.ofMillis(200);
        if (retryBackoffMax == null) retryBackoffMax = Duration.ofSeconds(5);
        if (maxAttempts <= 0) maxAttempts = 3;
    }
}
