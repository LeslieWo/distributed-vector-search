package dev.vectorsearch.coordinator;

import dev.vectorsearch.common.cluster.RedisClusterView;
import dev.vectorsearch.coordinator.cache.QueryCache;
import dev.vectorsearch.coordinator.grpc.CoordinatorGrpcServer;
import dev.vectorsearch.coordinator.search.SearchServiceImpl;
import dev.vectorsearch.coordinator.search.ShardSearcher;
import dev.vectorsearch.embedding.Embedder;
import dev.vectorsearch.embedding.HashingEmbedder;
import dev.vectorsearch.embedding.MiniLmEmbedder;
import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.ViewAcker;
import dev.vectorsearch.coordinator.search.RoundRobinSelector;
import dev.vectorsearch.coordinator.search.ScatterGather;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(CoordinatorProperties.class)
public class CoordinatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(CoordinatorApplication.class, args);
    }

    /** Same model as embed-worker: query and passage vectors must live in the same space. */
    @Bean
    Embedder embedder(CoordinatorProperties props) {
        return switch (props.embedder()) {
            case "onnx" -> new MiniLmEmbedder(props.modelDir());
            case "hashing" -> new HashingEmbedder(MiniLmEmbedder.DIMENSION);
            default -> throw new IllegalArgumentException("coordinator.embedder must be onnx or hashing");
        };
    }

    @Bean
    RedisClusterView clusterView(CoordinatorProperties props) {
        return new RedisClusterView(props.redisUri(), props.clusterPoll());
    }

    /** Null (no bean) when coordinator.cache-enabled=false. */
    @Bean
    QueryCache queryCache(CoordinatorProperties props, MeterRegistry registry) {
        if (!props.cacheEnabled()) return null;
        QueryCache cache = new QueryCache(props.redisUri(), props.cacheTtl(), props.cacheTimeout());
        FunctionCounter.builder("vs.coord.cache.hits", cache, c -> c.hits.get()).register(registry);
        FunctionCounter.builder("vs.coord.cache.misses", cache, c -> c.misses.get()).register(registry);
        FunctionCounter.builder("vs.coord.cache.errors", cache, c -> c.errors.get()).register(registry);
        return cache;
    }

    @Bean
    ChannelPool shardChannels() {
        return new ChannelPool(addr -> NettyChannelBuilder.forTarget(addr).usePlaintext().maxInboundMessageSize(32 << 20).build());
    }

    @Bean
    ShardSearcher shardSearcher(CoordinatorProperties props, RedisClusterView cluster, ChannelPool channels) {
        return new ScatterGather(cluster, channels, new RoundRobinSelector(), props.efSearch(), props.responseReserve(), props.shardTimeout());
    }

    /**
     * Tells a rebalance which ring this coordinator reads (step 27): old copies are deleted only after every
     * reader moved to the new ring. Every query calls cluster.view() when it starts, so queries starting after
     * an ack use at least that version; older in-flight ones are covered by the rebalancer's grace period.
     */
    @Bean
    ViewAcker readerAcker(CoordinatorProperties props, RedisClusterView cluster) {
        String id = ManagementFactory.getRuntimeMXBean().getName();
        return new ViewAcker(props.redisUri(), ViewAcker.READER, id,
                () -> ViewAcker.state(cluster.view().ringVersion(), -1), Duration.ofSeconds(1), Duration.ofSeconds(3));
    }

    @Bean
    SearchServiceImpl searchService(CoordinatorProperties props, Embedder embedder, ShardSearcher searcher,
                                    org.springframework.beans.factory.ObjectProvider<QueryCache> cache, MeterRegistry registry) {
        return new SearchServiceImpl(props, embedder, searcher, cache.getIfAvailable(), registry);
    }

    @Bean
    CoordinatorGrpcServer grpcServer(CoordinatorProperties props, SearchServiceImpl service) {
        return new CoordinatorGrpcServer(props.grpcPort(), props.grpcThreads(), service);
    }
}
