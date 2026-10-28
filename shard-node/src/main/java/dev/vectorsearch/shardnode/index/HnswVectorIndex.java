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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
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

    /** Re-deliveries within this distance count as "the same vector" (BUILDLOG step 18). */
    static final float DUPLICATE_TOLERANCE = 1e-4f;
    /**
     * Plus this fraction of the stored component: a vector restored from an fp16 snapshot differs from the fp32
     * re-delivery by up to 2^-11 of each component (BUILDLOG step 37). Real updates differ by far more.
     */
    static final float DUPLICATE_RELATIVE_TOLERANCE = 1e-3f;

    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong replaced = new AtomicLong();
    private final AtomicLong skippedPresent = new AtomicLong();

    // Upserts of the same id are serialized (check-then-add must be atomic, e.g. a rebalance copy with
    // onlyIfAbsent racing a live write); different ids almost always take different stripes.
    private final Object[] idLocks = new Object[256];

    {
        for (int i = 0; i < idLocks.length; i++) idLocks[i] = new Object();
    }

    @Override
    public void upsert(String id, float[] vector, String text) {
        upsert(id, vector, text, false);
    }

    /** @param onlyIfAbsent leave an existing item with this id untouched (rebalance copies, BUILDLOG step 27) */
    public void upsert(String id, float[] vector, String text, boolean onlyIfAbsent) {
        synchronized (idLocks[(id.hashCode() & 0x7fffffff) % idLocks.length]) {
            upsertLocked(id, vector, text, onlyIfAbsent);
        }
    }

    private void upsertLocked(String id, float[] vector, String text, boolean onlyIfAbsent) {
        ChunkItem existing = index.get(id).orElse(null);
        if (existing != null && onlyIfAbsent) {
            skippedPresent.incrementAndGet();
            return;
        }
        if (existing != null) {
            if (Objects.equals(existing.text(), text) && nearlyEqual(existing.vector(), vector)) {
                // Duplicate delivery (at-least-once). Skip: hnswlib would treat a vector that differs in the
                // last float bit (e.g. embedded on another CPU) as an update and leave a dead node behind.
                duplicates.incrementAndGet();
                return;
            }
            replaced.incrementAndGet(); // real update: hnswlib soft-deletes the old node and adds a new one
        }
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

    private static boolean nearlyEqual(float[] a, float[] b) {
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) if (Math.abs(a[i] - b[i]) > DUPLICATE_TOLERANCE + DUPLICATE_RELATIVE_TOLERANCE * Math.abs(a[i])) return false;
        return true;
    }

    /** Upserts skipped as duplicates since this index object was created. */
    public long duplicateUpserts() {
        return duplicates.get();
    }

    /** onlyIfAbsent upserts skipped because the id was already there. */
    public long skippedPresent() {
        return skippedPresent.get();
    }

    /** Upserts that replaced an existing vector (each leaves one soft-deleted node in the graph). */
    public long replacedVectors() {
        return replaced.get();
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

    /** Snapshot of the live items (soft-deleted ones excluded). Used by scans for rebalancing and checks. */
    public List<ChunkItem> items() {
        resizeLock.readLock().lock();
        try {
            return new ArrayList<>(index.items());
        } finally {
            resizeLock.readLock().unlock();
        }
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
        save(out, ChunkItem.DiskFormat.JAVA);
    }

    /** @param format how each item is written (step 37: fp16 vectors on disk, fp32 in memory) */
    public void save(OutputStream out, ChunkItem.DiskFormat format) throws IOException {
        resizeLock.readLock().lock();
        ChunkItem.SAVING.set(format);
        try {
            index.save(out);
        } finally {
            ChunkItem.SAVING.remove();
            resizeLock.readLock().unlock();
        }
    }

    public static HnswVectorIndex load(InputStream in, int defaultEf) throws IOException {
        HnswIndex<String, float[], ChunkItem, Float> loaded = HnswIndex.load(in, HnswVectorIndex.class.getClassLoader());
        return new HnswVectorIndex(loaded, defaultEf);
    }
}
