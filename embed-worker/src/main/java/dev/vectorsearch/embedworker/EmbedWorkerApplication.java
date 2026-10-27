package dev.vectorsearch.embedworker;

import dev.vectorsearch.common.kafka.TopicInitializer;
import dev.vectorsearch.embedding.Embedder;
import dev.vectorsearch.embedding.HashingEmbedder;
import dev.vectorsearch.embedding.MiniLmEmbedder;
import dev.vectorsearch.embedworker.consumer.ChunkConsumer;
import dev.vectorsearch.embedworker.consumer.WorkerMetrics;
import dev.vectorsearch.embedworker.pipeline.ChunkEmbedder;
import dev.vectorsearch.embedworker.shard.ShardWriter;
import dev.vectorsearch.embedworker.shard.SingleShardWriter;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(WorkerProperties.class)
public class EmbedWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(EmbedWorkerApplication.class, args);
    }

    /** "onnx" = all-MiniLM-L6-v2 (real), "hashing" = deterministic fake for tests and load tests. */
    @Bean
    Embedder embedder(WorkerProperties props) {
        return switch (props.embedder()) {
            case "onnx" -> new MiniLmEmbedder(props.modelDir());
            case "hashing" -> new HashingEmbedder(MiniLmEmbedder.DIMENSION);
            default -> throw new IllegalArgumentException("worker.embedder must be onnx or hashing");
        };
    }

    /** Phase 2: one configured shard-node. Steps 21-23 swap in the consistent-hash ring router here. */
    @Bean
    ShardWriter shardWriter(WorkerProperties props) {
        String[] hp = props.shardTarget().split(":");
        return new SingleShardWriter(
                NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().maxInboundMessageSize(32 << 20).build(),
                props.shardPartition(), props.shardDeadline().toMillis());
    }

    @Bean
    ChunkConsumer chunkConsumer(WorkerProperties props, Embedder embedder, ShardWriter writer, MeterRegistry registry) {
        TopicInitializer.ensure(props.bootstrapServers(), props.replicationFactor());
        return new ChunkConsumer(props, new ChunkEmbedder(embedder), writer, new WorkerMetrics(registry));
    }
}
