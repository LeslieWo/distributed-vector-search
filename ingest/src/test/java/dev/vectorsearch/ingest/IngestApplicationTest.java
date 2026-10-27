package dev.vectorsearch.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.kafka.RawChunkCodec;
import dev.vectorsearch.common.kafka.Topics;
import dev.vectorsearch.proto.v1.RawChunk;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IngestApplicationTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry r) {
        r.add("ingest.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    TestRestTemplate http;

    @Test
    void documentsAreChunkedAndLandOnRawChunksKeyedByChunkId() {
        String longText = IntStream.range(0, 300).mapToObj(i -> "w" + i).collect(Collectors.joining(" ")); // 3 chunks
        Map<String, Object> body = Map.of("documents", List.of(
                Map.of("docId", "short-1", "text", "what is the capital of france"),
                Map.of("docId", "long-1", "text", longText)));
        ResponseEntity<DocumentController.IngestResponse> resp = http.postForEntity("/documents", body, DocumentController.IngestResponse.class);
        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        assertEquals(4, resp.getBody().chunks());
        assertEquals(List.of("short-1#0", "long-1#0", "long-1#1", "long-1#2"), resp.getBody().chunkIds());

        Map<String, ConsumerRecord<String, byte[]>> seen = new HashMap<>();
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "group.id", "test", "auto.offset.reset", "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(Topics.RAW_CHUNKS));
            for (int i = 0; i < 40 && seen.size() < 4; i++) c.poll(Duration.ofMillis(250)).forEach(r -> seen.put(r.key(), r));
            c.poll(Duration.ofMillis(500)).forEach(r -> seen.put(r.key(), r)); // anything extra (e.g. from a rejected request)?
        }
        assertEquals(java.util.Set.of("short-1#0", "long-1#0", "long-1#1", "long-1#2"), seen.keySet());
        RawChunk second = RawChunkCodec.decode(seen.get("long-1#1").value());
        assertEquals("long-1", second.getDocId());
        assertEquals(1, second.getChunkIndex());
        assertTrue(second.getText().startsWith("w96 w97")); // window 128, overlap 32 -> second chunk starts at token 96
        assertTrue(second.getIngestedAtMs() > 0);
    }

    @Test
    void invalidRequestsAre400AndPublishNothing() {
        assertEquals(HttpStatus.BAD_REQUEST, http.postForEntity("/documents", Map.of("documents", List.of()), String.class).getStatusCode());
        ResponseEntity<String> r = http.postForEntity("/documents", Map.of("documents", List.of(
                Map.of("docId", "ok", "text", "fine"),
                Map.of("docId", "a#b", "text", "bad id"))), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatusCode());
        assertTrue(r.getBody().contains("documents[1].docId"));
    }
}
