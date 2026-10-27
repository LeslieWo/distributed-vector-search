package dev.vectorsearch.coordinator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(properties = {"coordinator.grpc-port=0", "server.port=0", "coordinator.embedder=hashing"})
class CoordinatorApplicationTest {
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void redisUri(DynamicPropertyRegistry r) {
        r.add("coordinator.redis-uri", () -> "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
    }

    @Test
    void contextLoads() {}
}
