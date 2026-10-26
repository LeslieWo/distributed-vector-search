package dev.vectorsearch.shardnode.index;

/** One search result; {@code score} is the dot product (= cosine for unit vectors), higher is better. */
public record Hit(String id, float score, String text) {}
