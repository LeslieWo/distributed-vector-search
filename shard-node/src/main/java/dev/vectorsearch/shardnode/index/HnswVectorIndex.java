package dev.vectorsearch.shardnode.index;

import com.github.jelmerk.hnswlib.core.DistanceFunctions;
import com.github.jelmerk.hnswlib.core.SearchResult;
import com.github.jelmerk.hnswlib.core.hnsw.HnswIndex;
import com.github.jelmerk.hnswlib.core.hnsw.SizeLimitExceededException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * HNSW (hnswlib-core) over unit vectors with inner-product distance (1 - dot).
 *
 * <p>Per-query efSearch: hnswlib-core only has an index-wide {@code ef} and searches with
 * {@code max(ef, k)} candidates. We keep the index-wide ef at 1 and ask for {@code max(k, ef)}
 * results, then cut to k. That is exactly an efSearch=ef search, and it is thread-safe.
 *
 * <p>Deletes in hnswlib-core are soft (the node stays in the graph, marked deleted), and replacing
 * a vector = delete + add, so capacity counts dead nodes too. We grow capacity on demand.
 */
public final class HnswVectorIndex implements VectorIndex {
    public record Params(int m, int efConstruction, int efSearch) {
        public static final Params DEFAULT = new Params(16, 200, 80);
    }

    private final HnswIndex<String, float[], ChunkItem, Float> index;
    private final int defaultEf;
    // Searches/adds hold the read lock (hnswlib handles that concurrency); resize needs the write lock
    // because it swaps the internal node array and visited-set pool under concurrent searches.
    private final ReentrantReadWriteLock resizeLock = new ReentrantReadWriteLock();

    public HnswVectorIndex(int dim, Params params, int initialCapacity) {
        this(HnswIndex.newBuilder(dim, DistanceFunctions.FLOAT_INNER_PRODUCT, Math.max(16, initialCapacity))
                .withM(params.m())
                .withEfConstruction(params.efConstruction())
                .withEf(1)
                .withRemoveEnabled()
                .build(), params.efSearch());
    }

    private HnswVectorIndex(HnswIndex<String, float[], ChunkItem, Float> index, int defaultEf) {
        this.index = index;
        this.defaultEf = defaultEf;
    }

    @Override
    public void upsert(String id, float[] vector, String text) {
        ChunkItem item = new ChunkItem(id, vector, text);
        while (true) {
            resizeLock.readLock().lock();
            try {
                index.add(item); // same id + identical vector: item replaced in place, no new node
                return;
            } catch (SizeLimitExceededException full) {
                // fall through and grow
            } finally {
                resizeLock.readLock().unlock();
            }
            grow();
        }
    }

    private void grow() {
        resizeLock.writeLock().lock();
        try {
            index.resize(index.getMaxItemCount() * 2);
        } finally {
            resizeLock.writeLock().unlock();
        }
    }

    @Override
    public boolean delete(String id) {
        resizeLock.readLock().lock();
        try {
            return index.remove(id, 0);
        } finally {
            resizeLock.readLock().unlock();
        }
    }

    @Override
    public List<Hit> search(float[] query, int k, int ef) {
        int candidates = Math.max(k, ef > 0 ? ef : defaultEf);
        List<SearchResult<ChunkItem, Float>> raw;
        resizeLock.readLock().lock();
        try {
            raw = index.findNearest(query, candidates);
        } finally {
            resizeLock.readLock().unlock();
        }
        List<Hit> hits = new ArrayList<>(Math.min(k, raw.size()));
        for (int i = 0; i < raw.size() && i < k; i++) {
            SearchResult<ChunkItem, Float> r = raw.get(i);
            hits.add(new Hit(r.item().id(), 1f - r.distance(), r.item().text()));
        }
        return hits;
    }

    @Override
    public int size() {
        return index.size();
    }

    @Override
    public int dimension() {
        return index.getDimensions();
    }

    public int defaultEf() {
        return defaultEf;
    }

    /**
     * Java-serialized graph + items. Searches may continue while saving, but callers must stop
     * writes (upsert/delete) for a consistent copy; the shard node's snapshotter does that.
     */
    public void save(OutputStream out) throws IOException {
        resizeLock.readLock().lock();
        try {
            index.save(out);
        } finally {
            resizeLock.readLock().unlock();
        }
    }

    public static HnswVectorIndex load(InputStream in, int defaultEf) throws IOException {
        HnswIndex<String, float[], ChunkItem, Float> loaded = HnswIndex.load(in, HnswVectorIndex.class.getClassLoader());
        return new HnswVectorIndex(loaded, defaultEf);
    }
}
