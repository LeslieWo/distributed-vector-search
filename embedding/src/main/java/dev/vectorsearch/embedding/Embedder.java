package dev.vectorsearch.embedding;

import java.util.List;

/** Turns texts into L2-normalized vectors, so dot product == cosine similarity. */
public interface Embedder extends AutoCloseable {
    int dimension();

    /** One vector per input text, in input order. */
    float[][] embed(List<String> texts);

    default float[] embed(String text) {
        return embed(List.of(text))[0];
    }

    @Override
    default void close() {}
}
