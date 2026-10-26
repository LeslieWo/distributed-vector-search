package dev.vectorsearch.common.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Proves the Testcontainers + colima setup works: throwaway KRaft broker, produce and consume one record. */
@Testcontainers
class KafkaContainerSmokeTest {

    // Same image as docker-compose, so no extra download.
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @Test
    void produceAndConsumeOneRecord() throws Exception {
        String topic = "smoke-" + UUID.randomUUID();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
        try (KafkaProducer<String, String> p = new KafkaProducer<>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers()), new StringSerializer(), new StringSerializer())) {
            p.send(new ProducerRecord<>(topic, "d1#0", "hello")).get();
        }
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "smoke",
                "auto.offset.reset", "earliest"), new StringDeserializer(), new StringDeserializer())) {
            c.subscribe(List.of(topic));
            ConsumerRecords<String, String> recs = ConsumerRecords.empty();
            for (int i = 0; i < 20 && recs.isEmpty(); i++) recs = c.poll(Duration.ofMillis(500));
            assertEquals(1, recs.count());
            assertEquals("hello", recs.iterator().next().value());
        }
    }
}
