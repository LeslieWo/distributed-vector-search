package dev.vectorsearch.shardnode;

import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code shard.*} settings; defaults are for shard-node-1 on a laptop. */
@ConfigurationProperties(prefix = "shard")
public record ShardNodeProperties(
        String nodeId,
        int grpcPort,
        int dimension,
        int hnswM,
        int hnswEfConstruction,
        int hnswEfSearch,
        int initialCapacity,
        int maxK,
        int grpcThreads,
        Path dataDir,
        Duration snapshotInterval) {

    public ShardNodeProperties {
        if (nodeId == null || nodeId.isBlank()) nodeId = "node-1";
        if (grpcPort < 0) throw new IllegalArgumentException("shard.grpc-port must be >= 0");
        if (dimension <= 0) dimension = 384;
        if (hnswM <= 0) hnswM = 16;
        if (hnswEfConstruction <= 0) hnswEfConstruction = 200;
        if (hnswEfSearch <= 0) hnswEfSearch = 80;
        if (initialCapacity <= 0) initialCapacity = 100_000;
        if (maxK <= 0) maxK = 1000;
        if (grpcThreads <= 0) grpcThreads = Runtime.getRuntime().availableProcessors() * 2;
        if (dataDir == null) dataDir = Path.of("data/snapshots", nodeId);
        if (snapshotInterval == null) snapshotInterval = Duration.ofMinutes(5);
    }
}
