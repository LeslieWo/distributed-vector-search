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
import dev.vectorsearch.coordinator.search.AdaptiveHedgePolicy;
import dev.vectorsearch.coordinator.search.HedgeBudget;
import dev.vectorsearch.coordinator.search.HedgePolicy;
import dev.vectorsearch.coordinator.search.NodeHealth;
import dev.vectorsearch.coordinator.search.NodeStats;
import dev.vectorsearch.coordinator.search.PowerOfTwoChoicesSelector;
import dev.vectorsearch.coordinator.search.ReplicaSelector;
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
            case "onnx" -> new MiniLmEmbedder(props.modelDir(), MiniLmEmbedder.DEFAULT_MAX_TOKENS, MiniLmEmbedder.DEFAULT_MICRO_BATCH, props.intraOpThreads());
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
    NodeStats nodeStats() {
        return new NodeStats();
    }

    /** Which holder of a partition gets the request: p2c (latency x in-flight) or round-robin (step 33). */
    @Bean
    ReplicaSelector replicaSelector(CoordinatorProperties props, NodeStats stats) {
        return switch (props.replicaSelection()) {
            case "p2c" -> new PowerOfTwoChoicesSelector(stats);
            case "round-robin" -> new RoundRobinSelector();
            default -> throw new IllegalArgumentException("coordinator.replica-selection must be p2c or round-robin");
        };
    }

    /** Hedged requests (step 34): adaptive per-partition p95 delay with a floor, capped at hedge-budget-ratio extra load. */
    @Bean
    ShardSearcher shardSearcher(CoordinatorProperties props, RedisClusterView cluster, ChannelPool channels, ReplicaSelector selector,
                                NodeStats stats, MeterRegistry registry) {
        HedgePolicy hedge = props.hedging()
                ? new AdaptiveHedgePolicy(props.hedgeMinDelay().toNanos(), props.hedgeInitialDelay().toNanos()) : null;
        ScatterGather sg = new ScatterGather(cluster, channels, selector, stats, new NodeHealth(), props.efSearch(), props.responseReserve(),
                props.shardTimeout(), hedge, new HedgeBudget(props.hedgeBudgetRatio(), 10), registry);
        sg.setCancelHedgeLosers(props.hedgeCancelLosers());
        return sg;
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
