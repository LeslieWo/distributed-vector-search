package dev.vectorsearch.embedworker.consumer;

import dev.vectorsearch.common.kafka.Topics;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Copies a poison record to raw-chunks.dlq: same key and value bytes (so it can be replayed as is
 * after a fix), plus headers saying why it failed and where it came from.
 */
public class DeadLetterPublisher implements AutoCloseable {
    public static final String REASON = "dlq.reason";
    public static final String ERROR = "dlq.error";
    public static final String ATTEMPTS = "dlq.attempts";
    public static final String SOURCE = "dlq.source"; // topic-partition@offset

    private final Producer<String, byte[]> producer;

    public DeadLetterPublisher(String bootstrapServers) {
        this(new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
                new StringSerializer(), new ByteArraySerializer()));
    }

    DeadLetterPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    /** Blocks until the DLQ write is acknowledged; only then may the source offset be committed. */
    public void publish(ConsumerRecord<String, byte[]> source, String reason, String error, int attempts) {
        ProducerRecord<String, byte[]> out = new ProducerRecord<>(Topics.RAW_CHUNKS_DLQ, source.key(), source.value());
        Headers h = out.headers();
        h.add(REASON, utf8(reason));
        h.add(ERROR, utf8(error == null ? "" : error.length() > 2000 ? error.substring(0, 2000) : error));
        h.add(ATTEMPTS, utf8(String.valueOf(attempts)));
        h.add(SOURCE, utf8(source.topic() + "-" + source.partition() + "@" + source.offset()));
        try {
            producer.send(out).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while writing to DLQ", e);
        } catch (Exception e) {
            throw new IllegalStateException("DLQ write failed: " + e, e);
        }
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        producer.close();
    }
}
