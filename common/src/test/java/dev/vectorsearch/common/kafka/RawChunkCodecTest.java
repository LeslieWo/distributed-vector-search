package dev.vectorsearch.common.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.common.chunk.Chunker;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RawChunkCodecTest {
    @Test
    void roundTripsAndRejectsGarbage() {
        Chunk c = new Chunker().chunk("42", "some passage text").get(0);
        assertEquals(c, RawChunkCodec.toChunk(RawChunkCodec.decode(RawChunkCodec.encode(c, 123))));
        assertEquals(123, RawChunkCodec.decode(RawChunkCodec.encode(c, 123)).getIngestedAtMs());
        assertThrows(IllegalArgumentException.class, () -> RawChunkCodec.decode("{\"json\":true}".getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> RawChunkCodec.decode(new byte[0])); // parses, but no chunk_id
        assertThrows(IllegalArgumentException.class, () -> RawChunkCodec.decode(null));
    }
}
