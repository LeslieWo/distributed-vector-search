package dev.vectorsearch.common.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.vectorsearch.common.ChunkId;
import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.proto.v1.RawChunk;

/** Chunk <-> RawChunk protobuf bytes, the value format of raw-chunks. */
public final class RawChunkCodec {
    private RawChunkCodec() {}

    public static byte[] encode(Chunk c, long ingestedAtMs) {
        return RawChunk.newBuilder()
                .setChunkId(c.id().value()).setDocId(c.docId()).setChunkIndex(c.index())
                .setText(c.text()).setIngestedAtMs(ingestedAtMs)
                .build().toByteArray();
    }

    /** @throws IllegalArgumentException for bytes that are not a valid RawChunk (poison record) */
    public static RawChunk decode(byte[] value) {
        if (value == null) throw new IllegalArgumentException("null value");
        try {
            RawChunk r = RawChunk.parseFrom(value);
            if (r.getChunkId().isBlank()) throw new IllegalArgumentException("missing chunk_id");
            return r;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("not a RawChunk: " + e.getMessage(), e);
        }
    }

    public static Chunk toChunk(RawChunk r) {
        return new Chunk(new ChunkId(r.getChunkId()), r.getDocId(), r.getChunkIndex(), r.getText());
    }
}
