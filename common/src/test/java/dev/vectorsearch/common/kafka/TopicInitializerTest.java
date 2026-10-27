package dev.vectorsearch.common.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.common.chunk.Chunker;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class TopicInitializerTest {
    @Container
    // per-test broker (non-static): the second test deliberately breaks the topic layout
    final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @Test
    void createsTopicsIdempotentlyAndKeyPinsPartition() throws Exception {
        assertTrue(TopicInitializer.ensure(KAFKA.getBootstrapServers(), (short) 1).isEmpty());
        assertTrue(TopicInitializer.ensure(KAFKA.getBootstrapServers(), (short) 1).isEmpty()); // second run: no error

        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            Map<String, TopicDescription> d = admin.describeTopics(List.of(Topics.RAW_CHUNKS, Topics.RAW_CHUNKS_DLQ)).allTopicNames().get();
            assertEquals(12, d.get(Topics.RAW_CHUNKS).partitions().size());
            assertEquals(3, d.get(Topics.RAW_CHUNKS_DLQ).partitions().size());
        }

        // Same key -> same partition, every time (per-chunk ordering); different keys spread out.
        Set<Integer> partitionsUsed = new HashSet<>();
        try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer())) {
            Chunk c = new Chunker().chunk("doc-1", "hello world").get(0);
            int first = p.send(new ProducerRecord<>(Topics.RAW_CHUNKS, c.id().value(), RawChunkCodec.encode(c, 0))).get().partition();
            for (int i = 0; i < 5; i++) {
                assertEquals(first, p.send(new ProducerRecord<>(Topics.RAW_CHUNKS, c.id().value(), RawChunkCodec.encode(c, i))).get().partition());
            }
            for (int i = 0; i < 200; i++) {
                partitionsUsed.add(p.send(new ProducerRecord<>(Topics.RAW_CHUNKS, "d" + i + "#0", new byte[0])).get().partition());
            }
        }
        assertEquals(12, partitionsUsed.size(), "200 distinct keys should hit all 12 partitions");
    }

    @Test
    void reportsExistingTopicWithWrongPartitionCount() throws Exception {
        TopicInitializer.ensure(KAFKA.getBootstrapServers(), (short) 1); // tests may run in any order
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.deleteTopics(List.of(Topics.RAW_CHUNKS_DLQ)).all().get();
            for (int i = 0; i < 50; i++) { // deletion is asynchronous on the broker
                try {
                    admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(Topics.RAW_CHUNKS_DLQ, 1, (short) 1))).all().get();
                    break;
                } catch (java.util.concurrent.ExecutionException e) {
                    Thread.sleep(200);
                }
            }
            List<String> problems = TopicInitializer.ensure(admin, (short) 1);
            assertEquals(1, problems.size(), problems.toString());
            assertTrue(problems.get(0).contains("raw-chunks.dlq has 1 partitions"));
        }
    }
}
