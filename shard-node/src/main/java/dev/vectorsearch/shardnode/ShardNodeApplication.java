package dev.vectorsearch.shardnode;

import dev.vectorsearch.shardnode.cluster.MembershipLifecycle;
import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import dev.vectorsearch.shardnode.grpc.ShardGrpcService;
import dev.vectorsearch.shardnode.metrics.GrpcMetricsInterceptor;
import dev.vectorsearch.shardnode.metrics.ShardMetrics;
import dev.vectorsearch.shardnode.store.PartitionStore;
import dev.vectorsearch.shardnode.store.PersistentPartitionStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(ShardNodeProperties.class)
public class ShardNodeApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShardNodeApplication.class, args);
    }

    /** Restores snapshots + WAL in its constructor, before the gRPC server starts; closed after it stops. */
    @Bean
    PersistentPartitionStore partitionStore(ShardNodeProperties props) {
        return new PersistentPartitionStore(props);
    }

    @Bean
    ShardMetrics shardMetrics(MeterRegistry registry, PersistentPartitionStore store) {
        return new ShardMetrics(registry, store);
    }

    @Bean
    ShardGrpcService shardGrpcService(PartitionStore store, ShardMetrics metrics, MeterRegistry registry) {
        ShardGrpcService service = new ShardGrpcService(store, metrics::recordUpserted);
        Counter expired = Counter.builder("vs.shard.search.expired")
                .description("searches skipped because the caller's deadline had already passed").register(registry);
        service.setOnExpired(expired::increment);
        return service;
    }

    @Bean
    GrpcServerLifecycle grpcServer(ShardNodeProperties props, ShardGrpcService service, MeterRegistry registry) {
        return new GrpcServerLifecycle(props.grpcPort(), props.grpcThreads(), List.of(service),
                List.of(new GrpcMetricsInterceptor(registry)));
    }

    @Bean
    MembershipLifecycle membership(ShardNodeProperties props, GrpcServerLifecycle grpc) {
        return new MembershipLifecycle(props, grpc);
    }
}
