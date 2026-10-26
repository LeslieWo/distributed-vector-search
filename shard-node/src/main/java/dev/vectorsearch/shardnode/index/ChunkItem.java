package dev.vectorsearch.shardnode.index;

import com.github.jelmerk.hnswlib.core.Item;

/** What one HNSW node stores: the chunk id, its unit vector and the text returned with hits. */
public record ChunkItem(String id, float[] vector, String text) implements Item<String, float[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public int dimensions() {
        return vector.length;
    }
}
