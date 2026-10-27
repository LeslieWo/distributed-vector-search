package dev.vectorsearch.embedworker.shard;

import dev.vectorsearch.embedworker.pipeline.EmbeddedChunk;
import java.util.List;

/**
 * Where embedded chunks go. Returns only when every chunk is durably acknowledged by every target
 * it must reach; the consumer commits Kafka offsets only after that.
 *
 * <p>Seam for Phase 3: today {@link SingleShardWriter} sends everything to one configured node;
 * steps 21-23 add a consistent-hash implementation that picks primary + replica per chunk id.
 */
public interface ShardWriter extends AutoCloseable {

    /** @throws ShardWriteException with {@code retriable()} telling the caller whether to retry or give up */
    void write(List<EmbeddedChunk> chunks);

    @Override
    default void close() {}
}
