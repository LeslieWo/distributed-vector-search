package dev.vectorsearch.embedworker.shard;

import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import java.util.List;

/**
 * Where embedded chunks go. Returns only when every chunk is durably acknowledged by every target
 * it must reach; the consumer commits Kafka offsets only after that.
 *
 * <p>{@link RingShardWriter} sends each chunk to both holders of its partition on the consistent-hash ring.
 */
public interface ShardWriter extends AutoCloseable {

    /** @throws ShardWriteException with {@code retriable()} telling the caller whether to retry or give up */
    void write(List<EmbeddedChunk> chunks);

    @Override
    default void close() {}
}
