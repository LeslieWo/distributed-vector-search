package dev.vectorsearch.shardnode.index;

import java.util.List;

/** A k-nearest-neighbour index over unit vectors, keyed by chunk id. Implementations are thread-safe. */
public interface VectorIndex {

    /** Inserts or replaces the vector stored under {@code id} (idempotent for identical input). */
    void upsert(String id, float[] vector, String text);

    /** @return true if something was removed */
    boolean delete(String id);

    /**
     * @param ef search breadth for approximate indexes (ignored by exact ones); 0 = index default
     * @return up to k hits, best first
     */
    List<Hit> search(float[] query, int k, int ef);

    int size();

    int dimension();
}
