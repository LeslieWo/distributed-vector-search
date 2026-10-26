package dev.vectorsearch.common.dataset;

/** One source document as it comes from the corpus, before chunking. */
public record Passage(String docId, String text) {}
