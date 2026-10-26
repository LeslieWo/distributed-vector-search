package dev.vectorsearch.common.chunk;

import dev.vectorsearch.common.ChunkId;

/** A window of a document's text; {@code index} is its position within the document. */
public record Chunk(ChunkId id, String docId, int index, String text) {}
