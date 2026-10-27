package dev.vectorsearch.embedworker.pipeline;

/** A chunk after embedding, ready to be written to its shard(s). */
public record EmbeddedChunk(String chunkId, float[] vector, String text) {}
