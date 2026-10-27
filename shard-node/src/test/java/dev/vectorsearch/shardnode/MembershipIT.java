package dev.vectorsearch.shardnode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;

/** Step 24: a running shard-node registers host:actualGrpcPort with a TTL and deregisters on graceful stop. */
class MembershipIT {
    @TempDir
    Path dir;

    @Test
    void registersWithTtlAndDeregistersOnGracefulStop() throws Exception {
        try (GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            RedisClient client = RedisClient.create(uri);
            try (StatefulRedisConnection<String, String> conn = client.connect()) {
                ConfigurableApplicationContext app = new SpringApplicationBuilder(ShardNodeApplication.class).run(
                        "--shard.node-id=node-7", "--shard.grpc-port=0", "--server.port=0", "--shard.dimension=4",
                        "--shard.data-dir=" + dir, "--shard.redis-uri=" + uri, "--shard.deregister-grace=0s");
                int port = app.getBean(GrpcServerLifecycle.class).port();
                assertEquals("localhost:" + port, conn.sync().get("vs:nodes:node-7"));
                long ttl = conn.sync().pttl("vs:nodes:node-7");
                assertTrue(ttl > 0 && ttl <= 3000, "ttl " + ttl);
                Thread.sleep(1500); // refreshed every second, so it is still there after more than half the TTL
                assertTrue(conn.sync().pttl("vs:nodes:node-7") > 1000);
                app.close();
                assertNull(conn.sync().get("vs:nodes:node-7"), "graceful stop deletes the key");
            } finally {
                client.shutdown();
            }
        }
    }
}
