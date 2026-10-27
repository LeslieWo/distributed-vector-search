package dev.vectorsearch.embedworker.pipeline;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.embedding.Embedder;
import java.util.ArrayList;
import java.util.List;

/** One batch of chunks -> one embedder call -> embedded chunks in the same order. */
public final class ChunkEmbedder {
    private final Embedder embedder;

    public ChunkEmbedder(Embedder embedder) {
        this.embedder = embedder;
    }

    public List<EmbeddedChunk> embed(List<Chunk> chunks) {
        float[][] vectors = embedder.embed(chunks.stream().map(Chunk::text).toList());
        List<EmbeddedChunk> out = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) out.add(new EmbeddedChunk(chunks.get(i).id().value(), vectors[i], chunks.get(i).text()));
        return out;
    }

    public int dimension() {
        return embedder.dimension();
    }
}
