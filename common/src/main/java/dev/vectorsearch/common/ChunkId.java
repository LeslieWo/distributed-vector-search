package dev.vectorsearch.common;

/** Stable identifier of a text chunk; the unit we hash onto the ring. */
public record ChunkId(String value) {
    public ChunkId {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("blank chunk id");
    }
}
