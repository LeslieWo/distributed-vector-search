package dev.vectorsearch.shardnode;

import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import dev.vectorsearch.shardnode.grpc.ShardGrpcService;
import dev.vectorsearch.shardnode.store.PartitionStore;
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

    @Bean
    PartitionStore partitionStore(ShardNodeProperties props) {
        return new PartitionStore(props);
    }

    @Bean
    ShardGrpcService shardGrpcService(PartitionStore store) {
        return new ShardGrpcService(store);
    }

    @Bean
    GrpcServerLifecycle grpcServer(ShardNodeProperties props, ShardGrpcService service) {
        return new GrpcServerLifecycle(props.grpcPort(), props.grpcThreads(), List.of(service));
    }
}
