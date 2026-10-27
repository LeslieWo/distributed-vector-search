package dev.vectorsearch.embedworker.it;

import dev.vectorsearch.common.chunk.Chunker;
import dev.vectorsearch.common.kafka.RawChunkCodec;
import dev.vectorsearch.common.kafka.TopicInitializer;
import dev.vectorsearch.common.kafka.Topics;
import dev.vectorsearch.embedworker.EmbedWorkerApplication;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.kafka.KafkaContainer;

/** Shared harness: fresh Kafka per test, fake shard, worker app started/stopped like a process. */
abstract class WorkerIT {
    KafkaContainer kafka;
    FakeShard shard;

    @BeforeEach
    void up() throws Exception {
        kafka = new KafkaContainer("apache/kafka:3.9.1");
        kafka.start();
        TopicInitializer.ensure(kafka.getBootstrapServers(), (short) 1);
        shard = new FakeShard();
    }

    @AfterEach
    void down() {
        shard.close();
        kafka.stop();
    }

    ConfigurableApplicationContext startWorker(String... extra) {
        List<String> args = new ArrayList<>(List.of(
                "--server.port=0",
                "--worker.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--worker.embedder=hashing",
                "--worker.shard-target=" + shard.target(),
                "--worker.retry-backoff-initial=50ms",
                "--worker.retry-backoff-max=200ms"));
        args.addAll(List.of(extra));
        return new SpringApplicationBuilder(EmbedWorkerApplication.class).run(args.toArray(String[]::new));
    }

    /** Produces docs prefix0..prefix(n-1), one chunk each; returns their chunk ids. */
    List<String> produce(String prefix, int n) throws Exception {
        List<String> ids = new ArrayList<>();
        try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers", kafka.getBootstrapServers(), "acks", "all"),
                new StringSerializer(), new ByteArraySerializer())) {
            for (int i = 0; i < n; i++) {
                var c = new Chunker().chunk(prefix + i, "passage number " + i + " about topic " + (i % 17)).get(0);
                p.send(new ProducerRecord<>(Topics.RAW_CHUNKS, c.id().value(), RawChunkCodec.encode(c, System.currentTimeMillis())));
                ids.add(c.id().value());
            }
            p.flush();
        }
        return ids;
    }

    void produceRaw(String key, byte[] value) throws Exception {
        try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers", kafka.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer())) {
            p.send(new ProducerRecord<>(Topics.RAW_CHUNKS, key, value)).get();
        }
    }

    long committedTotal(String group) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            Map<?, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            return offsets.values().stream().filter(o -> o != null).mapToLong(OffsetAndMetadata::offset).sum();
        }
    }
}
