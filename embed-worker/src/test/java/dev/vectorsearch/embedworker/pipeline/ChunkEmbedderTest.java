package dev.vectorsearch.embedworker.pipeline;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vectorsearch.common.chunk.Chunker;
import dev.vectorsearch.embedding.HashingEmbedder;
import dev.vectorsearch.embedding.Vectors;
import java.util.List;
import org.junit.jupiter.api.Test;

class ChunkEmbedderTest {
    @Test
    void keepsOrderIdsAndTextAndIsDeterministic() {
        ChunkEmbedder e = new ChunkEmbedder(new HashingEmbedder(16));
        var chunks = List.of(new Chunker().chunk("a", "red apples").get(0), new Chunker().chunk("b", "blue sky").get(0));
        List<EmbeddedChunk> out = e.embed(chunks);
        assertEquals(List.of("a#0", "b#0"), out.stream().map(EmbeddedChunk::chunkId).toList());
        assertEquals("blue sky", out.get(1).text());
        assertEquals(1.0, Vectors.dot(out.get(0).vector(), out.get(0).vector()), 1e-5);
        assertArrayEquals(out.get(0).vector(), e.embed(chunks).get(0).vector());
        float shared = Vectors.dot(new HashingEmbedder(16).embed("red apples"), new HashingEmbedder(16).embed("red cars"));
        assertTrue(shared > 0.2f, "shared word should give similar vectors: " + shared);
    }
}
