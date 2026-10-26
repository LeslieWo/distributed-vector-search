package dev.vectorsearch.common.chunk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ChunkerTest {

    private static String words(int from, int toExclusive) {
        return IntStream.range(from, toExclusive).mapToObj(i -> "w" + i).collect(Collectors.joining(" "));
    }

    @Test
    void shortTextIsOneChunkWithIndexZero() {
        List<Chunk> chunks = new Chunker().chunk("42", "  hello   world\n");
        assertEquals(1, chunks.size());
        assertEquals("42#0", chunks.get(0).id().value());
        assertEquals("hello world", chunks.get(0).text());
    }

    @Test
    void windowsOverlapAndCoverEveryToken() {
        // window 4, overlap 1 -> stride 3: [0..4) [3..7) [6..10)
        List<Chunk> chunks = new Chunker(4, 1).chunk("d", words(0, 10));
        assertEquals(List.of(words(0, 4), words(3, 7), words(6, 10)), chunks.stream().map(Chunk::text).toList());
        assertEquals(List.of("d#0", "d#1", "d#2"), chunks.stream().map(c -> c.id().value()).toList());
    }

    @Test
    void doesNotEmitTailAlreadyCoveredByPreviousWindow() {
        // 7 tokens, window 4, overlap 1: [0..4) [3..7) and stop; no extra [6..7) chunk
        assertEquals(2, new Chunker(4, 1).chunk("d", words(0, 7)).size());
    }

    @Test
    void sameInputGivesSameIds() {
        String text = words(0, 300);
        assertEquals(new Chunker().chunk("x", text), new Chunker().chunk("x", text));
        assertEquals(3, new Chunker().chunk("x", text).size()); // 128 + 96 + 96 >= 300
    }

    @Test
    void emptyTextHasNoChunks() {
        assertTrue(new Chunker().chunk("d", "   ").isEmpty());
        assertTrue(new Chunker().chunk("d", null).isEmpty());
    }

    @Test
    void rejectsBadParametersAndIds() {
        assertThrows(IllegalArgumentException.class, () -> new Chunker(4, 4));
        assertThrows(IllegalArgumentException.class, () -> new Chunker().chunk("a#b", "text"));
    }
}
