package dev.vectorsearch.embedworker.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.chunk.Chunker;
import dev.vectorsearch.common.kafka.RawChunkCodec;
import dev.vectorsearch.common.kafka.Topics;
import dev.vectorsearch.embedworker.consumer.DeadLetterPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Step 20: poison records go to raw-chunks.dlq after bounded retries; the rest of their batch still lands. */
class DeadLetterIT extends WorkerIT {

    @Test
    void poisonRecordsAreDeadLetteredAndGoodOnesInTheSameBatchAreWritten() throws Exception {
        produce("good", 100);
        produceRaw("junk#0", "{\"not\":\"protobuf\"}".getBytes(StandardCharsets.UTF_8));
        produceRaw("empty#0", new byte[0]);
        var poison = new Chunker().chunk("bad", "this text contains POISON").get(0);
        produceRaw(poison.id().value(), RawChunkCodec.encode(poison, 0));
        shard.invalid = c -> c.getText().contains("POISON"); // shard answers INVALID_ARGUMENT, like a real validation error

        try (ConfigurableApplicationContext w = startWorker("--worker.batch-size=256", "--worker.batch-max-wait=1s")) {
            await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 103);
        }
        assertEquals(100, shard.chunkIds().size());
        assertFalse(shard.chunkIds().contains("bad#0"));

        Map<String, ConsumerRecord<String, byte[]>> dead = new HashMap<>();
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(), "group.id", "dlq-reader", "auto.offset.reset", "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(Topics.RAW_CHUNKS_DLQ));
            for (int i = 0; i < 40 && dead.size() < 3; i++) c.poll(Duration.ofMillis(250)).forEach(r -> dead.put(r.key(), r));
        }
        assertEquals(java.util.Set.of("junk#0", "empty#0", "bad#0"), dead.keySet());
        assertEquals("undecodable", header(dead.get("junk#0"), DeadLetterPublisher.REASON));
        assertEquals("1", header(dead.get("junk#0"), DeadLetterPublisher.ATTEMPTS));
        assertEquals("permanent-failure", header(dead.get("bad#0"), DeadLetterPublisher.REASON));
        assertEquals("3", header(dead.get("bad#0"), DeadLetterPublisher.ATTEMPTS));
        assertTrue(header(dead.get("bad#0"), DeadLetterPublisher.ERROR).contains("INVALID_ARGUMENT"));
        assertTrue(header(dead.get("bad#0"), DeadLetterPublisher.SOURCE).startsWith("raw-chunks-"));
        // value bytes are kept as is, so the record can be replayed after a fix
        assertEquals("this text contains POISON", RawChunkCodec.decode(dead.get("bad#0").value()).getText());
    }

    @Test
    void outagesAreRetriedNotDeadLettered() throws Exception {
        produce("d", 20);
        shard.failNextCalls.set(6); // infrastructure trouble, not bad data
        try (ConfigurableApplicationContext w = startWorker("--worker.max-attempts=2")) {
            await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 20);
            assertEquals(0.0, w.getBean(io.micrometer.core.instrument.MeterRegistry.class).counter("vs.worker.dlq").count());
        }
        assertEquals(20, shard.chunkIds().size());
    }

    static String header(ConsumerRecord<String, byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
