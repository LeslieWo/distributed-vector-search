package dev.vectorsearch.embedworker;

import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.ClusterViewSource;
import dev.vectorsearch.common.cluster.RedisClusterView;
import dev.vectorsearch.common.cluster.StaticClusterView;
import dev.vectorsearch.common.kafka.TopicInitializer;
import dev.vectorsearch.embedding.Embedder;
import dev.vectorsearch.embedding.HashingEmbedder;
import dev.vectorsearch.embedding.MiniLmEmbedder;
import dev.vectorsearch.embedworker.consumer.ChunkConsumer;
import dev.vectorsearch.embedworker.consumer.DeadLetterPublisher;
import dev.vectorsearch.embedworker.consumer.WorkerMetrics;
import dev.vectorsearch.embedworker.pipeline.ChunkEmbedder;
import dev.vectorsearch.embedworker.shard.ShardWriter;
import dev.vectorsearch.embedworker.shard.RingShardWriter;
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

    /** Ring + live nodes from Redis (step 24), or a fixed list when worker.static-nodes is set (tests). */
    @Bean
    ClusterViewSource clusterView(WorkerProperties props) {
        return props.staticNodes().isBlank()
                ? new RedisClusterView(props.redisUri(), props.clusterPoll())
                : StaticClusterView.parse(props.staticNodes());
    }

    /** Each chunk goes to both holders of its partition on the consistent-hash ring (step 23). */
    @Bean
    ShardWriter shardWriter(WorkerProperties props, ClusterViewSource cluster) {
        ChannelPool channels = new ChannelPool(addr -> NettyChannelBuilder.forTarget(addr).usePlaintext().maxInboundMessageSize(32 << 20).build());
        return new RingShardWriter(cluster, channels, props.shardDeadline().toMillis());
    }

    @Bean
    ChunkConsumer chunkConsumer(WorkerProperties props, Embedder embedder, ShardWriter writer, MeterRegistry registry) {
        TopicInitializer.ensure(props.bootstrapServers(), props.replicationFactor());
        return new ChunkConsumer(props, new ChunkEmbedder(embedder), writer,
                new DeadLetterPublisher(props.bootstrapServers()), new WorkerMetrics(registry));
    }
}
