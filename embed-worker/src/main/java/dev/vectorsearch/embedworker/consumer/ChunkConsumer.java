package dev.vectorsearch.embedworker.consumer;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.common.kafka.RawChunkCodec;
import dev.vectorsearch.common.kafka.Topics;
import dev.vectorsearch.embedworker.WorkerProperties;
import dev.vectorsearch.embedworker.batch.MicroBatcher;
import dev.vectorsearch.embedworker.pipeline.ChunkEmbedder;
import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import dev.vectorsearch.embedworker.shard.ShardWriteException;
import dev.vectorsearch.embedworker.shard.ShardWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The embed-worker loop: poll raw-chunks -> micro-batch -> embed -> write to shard(s) -> commit offsets.
 *
 * <p>At-least-once: offsets are committed only after the shard writer acknowledged the batch. A crash
 * anywhere before the commit means the records are delivered again (to this or another group member);
 * that is safe because Upsert is idempotent by chunk id. Nothing is committed that was not written.
 */
public class ChunkConsumer implements SmartLifecycle, Runnable {
    private static final Logger log = LoggerFactory.getLogger(ChunkConsumer.class);

    private final WorkerProperties props;
    private final ChunkEmbedder embedder;
    private final ShardWriter writer;
    private final WorkerMetrics metrics;
    private final MicroBatcher<ConsumerRecord<String, byte[]>> batcher;
    private final Set<TopicPartition> assigned = new HashSet<>();
    private KafkaConsumer<String, byte[]> consumer;
    private volatile boolean running;
    private Thread thread;

    public ChunkConsumer(WorkerProperties props, ChunkEmbedder embedder, ShardWriter writer, WorkerMetrics metrics) {
        this.props = props;
        this.embedder = embedder;
        this.writer = writer;
        this.metrics = metrics;
        this.batcher = new MicroBatcher<>(props.batchSize(), props.batchMaxWait().toMillis(), System::currentTimeMillis);
    }

    static Map<String, Object> consumerConfig(WorkerProperties p) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, p.groupId(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false, // we commit after the shard ack, never on a timer
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // a new group starts from the beginning
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, p.maxPollRecords(),
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) p.maxPollInterval().toMillis());
    }

    @Override
    public void start() {
        consumer = new KafkaConsumer<>(consumerConfig(props), new StringDeserializer(), new ByteArrayDeserializer());
        running = true;
        thread = Thread.ofPlatform().name("chunk-consumer").start(this);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(Topics.RAW_CHUNKS), new Rebalance());
            while (running) {
                if (!batcher.isFull()) {
                    for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(batcher.millisUntilDue(500)))) {
                        batcher.add(r);
                        metrics.records.increment();
                    }
                }
                if (batcher.ready()) processAndCommit(batcher.drain());
            }
            // graceful stop: finish what is buffered so the next owner does not redo it
            while (batcher.size() > 0) processAndCommit(batcher.drain());
        } catch (WakeupException e) {
            if (running) throw e;
        } catch (RuntimeException e) {
            log.error("consumer loop died; records since the last commit will be redelivered", e);
        } finally {
            consumer.close();
        }
    }

    void processAndCommit(List<ConsumerRecord<String, byte[]>> records) {
        // Same chunk id twice in one batch (re-ingest, redelivery): keep the latest, embed once.
        // Same key = same partition, and records arrive in offset order, so "last" = newest.
        Map<String, Chunk> latest = new LinkedHashMap<>();
        for (ConsumerRecord<String, byte[]> r : records) {
            try {
                Chunk c = RawChunkCodec.toChunk(RawChunkCodec.decode(r.value()));
                latest.remove(c.id().value());
                latest.put(c.id().value(), c);
            } catch (IllegalArgumentException poison) {
                log.warn("skipping undecodable record {}-{}@{}: {}", r.topic(), r.partition(), r.offset(), poison.getMessage());
            }
        }
        List<Chunk> chunks = new ArrayList<>(latest.values());
        if (!chunks.isEmpty()) {
            metrics.batchSize.record(chunks.size());
            List<EmbeddedChunk> embedded = metrics.embed.record(() -> embedder.embed(chunks));
            writeWithRetry(embedded);
            metrics.written.increment(embedded.size());
        }
        commit(records);
    }

    /**
     * Retries retriable failures forever with capped exponential backoff. While waiting, all partitions
     * are paused and we keep calling poll(): paused partitions return no records, but the poll keeps us
     * inside max.poll.interval.ms, so an outage does not get this worker kicked out of the group (which
     * would trigger a rebalance and redeliver everything to someone who would hit the same outage).
     */
    private void writeWithRetry(List<EmbeddedChunk> embedded) {
        long backoff = props.retryBackoffInitial().toMillis();
        boolean paused = false;
        try {
            while (true) {
                try {
                    metrics.write.record(() -> writer.write(embedded));
                    return;
                } catch (ShardWriteException e) {
                    if (!e.retriable()) throw e;
                    // shutting down: give up without committing; the batch is redelivered after restart
                    if (!running) throw new IllegalStateException("stopped while shard writes were failing", e);
                    metrics.retries.increment();
                    log.warn("shard write failed, retrying in {} ms: {}", backoff, e.getMessage());
                    paused = true;
                    waitPolling(backoff);
                    backoff = Math.min(backoff * 2, props.retryBackoffMax().toMillis());
                }
            }
        } finally {
            if (paused) consumer.resume(consumer.paused());
        }
    }

    private void waitPolling(long millis) {
        long until = System.currentTimeMillis() + millis;
        long left;
        while ((left = until - System.currentTimeMillis()) > 0) {
            // re-pause every time: a rebalance during the wait may have handed us new, unpaused partitions
            consumer.pause(consumer.assignment());
            for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(left))) {
                batcher.add(r); // only possible right after a rebalance; keep them, nothing is lost
                metrics.records.increment();
            }
        }
    }

    /** Commit offset+1 of the last record per partition, only for partitions we still own. */
    private void commit(List<ConsumerRecord<String, byte[]>> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, byte[]> r : records) {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            if (!assigned.contains(tp)) continue;
            OffsetAndMetadata next = new OffsetAndMetadata(r.offset() + 1);
            offsets.merge(tp, next, (a, b) -> a.offset() >= b.offset() ? a : b);
        }
        if (offsets.isEmpty()) return;
        try {
            consumer.commitSync(offsets);
            metrics.commits.increment();
        } catch (RuntimeException e) {
            // e.g. we were kicked out of the group: the records will simply be processed again
            log.warn("offset commit failed, records will be redelivered: {}", e.toString());
        }
    }

    private final class Rebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            assigned.removeAll(partitions);
            if (!partitions.isEmpty()) metrics.revocations.increment();
            // Buffered, uncommitted records of revoked partitions go to the new owner from the committed
            // offset; drop our copies instead of processing them twice.
            List<ConsumerRecord<String, byte[]>> keep = new ArrayList<>();
            for (ConsumerRecord<String, byte[]> r : batcher.drainAll()) {
                if (!partitions.contains(new TopicPartition(r.topic(), r.partition()))) keep.add(r);
            }
            keep.forEach(batcher::add);
            log.info("partitions revoked: {}", partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assigned.addAll(partitions);
            log.info("partitions assigned: {}", partitions);
        }
    }

    @Override
    public void stop() {
        running = false;
        if (consumer != null) consumer.wakeup();
        try {
            if (thread != null) thread.join(30_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
