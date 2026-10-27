package dev.vectorsearch.ingest;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.common.kafka.RawChunkCodec;
import dev.vectorsearch.common.kafka.Topics;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Sends chunks to raw-chunks and returns only after Kafka acknowledged all of them. */
public class ChunkPublisher implements AutoCloseable {
    private final Producer<String, byte[]> producer;
    private final long timeoutMillis;

    public ChunkPublisher(IngestProperties props) {
        this(new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers(),
                // acks=all + idempotence: a send counts only once it is on all in-sync replicas, and producer
                // retries after a lost ack do not create duplicates inside Kafka.
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.LINGER_MS_CONFIG, 5,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) props.sendTimeout().toMillis(),
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(10_000, (int) props.sendTimeout().toMillis())),
                new StringSerializer(), new ByteArraySerializer()), props.sendTimeout().toMillis());
    }

    ChunkPublisher(Producer<String, byte[]> producer, long timeoutMillis) {
        this.producer = producer;
        this.timeoutMillis = timeoutMillis;
    }

    /** @throws PublishException if any chunk was not acknowledged in time (the caller should retry the request) */
    public void publish(List<Chunk> chunks) {
        long now = System.currentTimeMillis();
        List<Future<RecordMetadata>> acks = new ArrayList<>(chunks.size());
        for (Chunk c : chunks) {
            acks.add(producer.send(new ProducerRecord<>(Topics.RAW_CHUNKS, c.id().value(), RawChunkCodec.encode(c, now))));
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        try {
            for (Future<RecordMetadata> f : acks) f.get(Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException("interrupted", e);
        } catch (Exception e) {
            throw new PublishException("kafka did not acknowledge all chunks: " + e, e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    public static class PublishException extends RuntimeException {
        PublishException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
