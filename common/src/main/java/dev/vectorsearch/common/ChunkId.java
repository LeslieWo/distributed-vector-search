package dev.vectorsearch.common;

/**
 * Stable identifier of a text chunk; the unit we hash onto the ring.
 * Format: {@code <docId>#<chunkIndex>}, e.g. {@code 8841823#0}.
 */
public record ChunkId(String value) {
    public ChunkId {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("blank chunk id");
    }

    public static ChunkId of(String docId, int chunkIndex) {
        if (docId == null || docId.isBlank() || docId.indexOf('#') >= 0) {
            throw new IllegalArgumentException("doc id must be non-blank and must not contain '#': " + docId);
        }
        if (chunkIndex < 0) throw new IllegalArgumentException("negative chunk index");
        return new ChunkId(docId + "#" + chunkIndex);
    }

    @Override
    public String toString() {
        return value;
    }
}
