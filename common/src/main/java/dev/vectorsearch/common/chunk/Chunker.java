package dev.vectorsearch.common.chunk;

import dev.vectorsearch.common.ChunkId;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Splits text into fixed-size windows of whitespace tokens with overlap.
 *
 * <p>Ids are {@code docId#i}: the same document text with the same window/overlap always
 * yields the same ids, so re-ingesting a document overwrites its chunks instead of duplicating them.
 *
 * <p>Tokens are whitespace-separated words, not model word-pieces, so ingest does not need the
 * model tokenizer. 128 words stays under all-MiniLM-L6-v2's 256 word-piece limit for normal English.
 */
public final class Chunker {
    public static final int DEFAULT_WINDOW = 128;
    public static final int DEFAULT_OVERLAP = 32;

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private final int window;
    private final int overlap;

    public Chunker() {
        this(DEFAULT_WINDOW, DEFAULT_OVERLAP);
    }

    public Chunker(int window, int overlap) {
        if (window <= 0) throw new IllegalArgumentException("window must be > 0");
        if (overlap < 0 || overlap >= window) throw new IllegalArgumentException("need 0 <= overlap < window");
        this.window = window;
        this.overlap = overlap;
    }

    public List<Chunk> chunk(String docId, String text) {
        String trimmed = text == null ? "" : text.strip();
        if (trimmed.isEmpty()) return List.of();
        String[] tokens = WHITESPACE.split(trimmed);
        int stride = window - overlap;
        List<Chunk> out = new ArrayList<>();
        for (int start = 0, i = 0; ; start += stride, i++) {
            int end = Math.min(start + window, tokens.length);
            out.add(new Chunk(ChunkId.of(docId, i), docId, i, String.join(" ", List.of(tokens).subList(start, end))));
            if (end == tokens.length) break;
        }
        return out;
    }
}
