package dev.vectorsearch.coordinator;

import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code coordinator.*} settings. */
@ConfigurationProperties(prefix = "coordinator")
public record CoordinatorProperties(
        int grpcPort,
        int grpcThreads,
        String redisUri,
        Duration clusterPoll,
        String embedder,
        Path modelDir,
        int defaultK,
        int maxK,
        int efSearch,
        Duration defaultDeadline,
        Duration responseReserve,
        Duration shardTimeout,
        String replicaSelection,
        boolean hedging,
        Duration hedgeMinDelay,
        Duration hedgeInitialDelay,
        double hedgeBudgetRatio,
        boolean cacheEnabled,
        Duration cacheTtl,
        Duration cacheTimeout,
        int intraOpThreads,
        Boolean hedgeCancelLosers) {

    public CoordinatorProperties {
        if (grpcPort < 0) throw new IllegalArgumentException("coordinator.grpc-port must be >= 0");
        if (grpcThreads <= 0) grpcThreads = 64;
        if (redisUri == null || redisUri.isBlank()) redisUri = "redis://localhost:6379";
        if (clusterPoll == null) clusterPoll = Duration.ofMillis(500);
        if (embedder == null || embedder.isBlank()) embedder = "onnx";
        if (modelDir == null) modelDir = Path.of("data/models/all-MiniLM-L6-v2");
        if (defaultK <= 0) defaultK = 10;
        if (maxK <= 0) maxK = 100;
        if (efSearch < 0) efSearch = 0; // 0 = each shard's default (80)
        if (defaultDeadline == null) defaultDeadline = Duration.ofSeconds(1);
        // shards must give up this much before the query deadline, so a partial answer still reaches the client
        if (responseReserve == null) responseReserve = Duration.ofMillis(5);
        // upper bound for one partition request even when the query deadline is far away
        if (shardTimeout == null) shardTimeout = Duration.ofMillis(500);
        if (replicaSelection == null || replicaSelection.isBlank()) replicaSelection = "p2c";
        if (hedgeMinDelay == null) hedgeMinDelay = Duration.ofMillis(3);
        if (hedgeInitialDelay == null) hedgeInitialDelay = Duration.ofMillis(10);
        if (hedgeBudgetRatio <= 0) hedgeBudgetRatio = 0.1;
        if (cacheTtl == null) cacheTtl = Duration.ofSeconds(60);
        if (cacheTimeout == null) cacheTimeout = Duration.ofMillis(50);
        if (intraOpThreads < 0) intraOpThreads = 0; // 0 = onnxruntime default (one thread per core)
        if (hedgeCancelLosers == null) hedgeCancelLosers = true;
    }
}
