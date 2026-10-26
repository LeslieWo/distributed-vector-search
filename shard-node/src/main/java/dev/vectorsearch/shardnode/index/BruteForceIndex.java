package dev.vectorsearch.shardnode.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Exact kNN: compares the query against every stored vector. O(n * d) per query, so it is the
 * correctness baseline and ground-truth generator, not something to serve 2M vectors with.
 * Vectors live in one flat float[] (slot i occupies [i*d, (i+1)*d)) to keep the scan cache friendly.
 */
public final class BruteForceIndex implements VectorIndex {
    private final int dim;
    private float[] data;
    private final List<String> ids = new ArrayList<>();
    private final List<String> texts = new ArrayList<>();
    private final Map<String, Integer> slotOf = new HashMap<>();

    public BruteForceIndex(int dim) {
        this(dim, 1024);
    }

    public BruteForceIndex(int dim, int initialCapacity) {
        this.dim = dim;
        this.data = new float[dim * Math.max(1, initialCapacity)];
    }

    @Override
    public synchronized void upsert(String id, float[] vector, String text) {
        if (vector.length != dim) throw new IllegalArgumentException("expected " + dim + " dims, got " + vector.length);
        Integer slot = slotOf.get(id);
        if (slot == null) {
            slot = ids.size();
            if ((slot + 1) * dim > data.length) data = Arrays.copyOf(data, data.length * 2);
            ids.add(id);
            texts.add(text);
            slotOf.put(id, slot);
        } else {
            texts.set(slot, text);
        }
        System.arraycopy(vector, 0, data, slot * dim, dim);
    }

    @Override
    public synchronized boolean delete(String id) {
        Integer slot = slotOf.remove(id);
        if (slot == null) return false;
        int last = ids.size() - 1;
        if (slot != last) { // move the last vector into the hole
            System.arraycopy(data, last * dim, data, slot * dim, dim);
            ids.set(slot, ids.get(last));
            texts.set(slot, texts.get(last));
            slotOf.put(ids.get(slot), slot);
        }
        ids.remove(last);
        texts.remove(last);
        return true;
    }

    @Override
    public synchronized List<Hit> search(float[] query, int k, int ef) {
        int[] top = topK(query, k);
        List<Hit> hits = new ArrayList<>(top.length);
        for (int slot : top) hits.add(new Hit(ids.get(slot), score(query, slot), texts.get(slot)));
        return hits;
    }

    /** Slots of the k best vectors, best first. Not synchronized: callers must not mutate concurrently. */
    public int[] topK(float[] query, int k) {
        int n = ids.size();
        // min-heap of the best k seen so far; the root is the weakest of them
        PriorityQueue<long[]> heap = new PriorityQueue<>(k + 1, (a, b) -> Float.compare(Float.intBitsToFloat((int) a[1]), Float.intBitsToFloat((int) b[1])));
        for (int slot = 0; slot < n; slot++) {
            float s = score(query, slot);
            if (heap.size() < k) {
                heap.add(new long[] {slot, Float.floatToRawIntBits(s)});
            } else if (s > Float.intBitsToFloat((int) heap.peek()[1])) {
                heap.poll();
                heap.add(new long[] {slot, Float.floatToRawIntBits(s)});
            }
        }
        int[] out = new int[heap.size()];
        for (int i = out.length - 1; i >= 0; i--) out[i] = (int) heap.poll()[0];
        return out;
    }

    private float score(float[] q, int slot) {
        int base = slot * dim;
        float s = 0f;
        for (int d = 0; d < dim; d++) s += q[d] * data[base + d];
        return s;
    }

    public String idAt(int slot) {
        return ids.get(slot);
    }

    @Override
    public synchronized int size() {
        return ids.size();
    }

    @Override
    public int dimension() {
        return dim;
    }
}
