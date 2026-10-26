package dev.vectorsearch.shardnode.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HnswVectorIndexTest {
    static final int DIM = 32;

    /** Clustered unit vectors: closer to real embeddings than uniform noise. */
    static float[][] clustered(Random r, int n) {
        float[][] centers = new float[20][];
        for (int c = 0; c < centers.length; c++) centers[c] = BruteForceIndexTest.unit(r, DIM);
        float[][] out = new float[n][DIM];
        for (int i = 0; i < n; i++) {
            float[] c = centers[r.nextInt(centers.length)];
            double norm = 0;
            for (int d = 0; d < DIM; d++) {
                out[i][d] = c[d] + 0.35f * (float) r.nextGaussian() / (float) Math.sqrt(DIM);
                norm += out[i][d] * out[i][d];
            }
            for (int d = 0; d < DIM; d++) out[i][d] /= (float) Math.sqrt(norm);
        }
        return out;
    }

    @Test
    void recallAt10AgainstBruteForceIsHigh() {
        Random r = new Random(42);
        float[][] data = clustered(r, 5000);
        HnswVectorIndex hnsw = new HnswVectorIndex(DIM, new HnswVectorIndex.Params(16, 100, 64), 100); // forces resizes
        BruteForceIndex exact = new BruteForceIndex(DIM, data.length);
        for (int i = 0; i < data.length; i++) {
            hnsw.upsert("c" + i, data[i], null);
            exact.upsert("c" + i, data[i], null);
        }
        assertEquals(5000, hnsw.size());
        float[][] queries = clustered(r, 200);
        double low = recall(hnsw, exact, queries, 10);
        double high = recall(hnsw, exact, queries, 200);
        System.out.printf("synthetic recall@10: ef=10 %.3f, ef=200 %.3f%n", low, high);
        // per-query ef must actually change the search breadth
        assertTrue(high > low, "ef=200 " + high + " vs ef=10 " + low);
        assertTrue(high > 0.97, "recall@10 at ef=200 = " + high);
    }

    static double recall(HnswVectorIndex hnsw, BruteForceIndex exact, float[][] queries, int ef) {
        double recall = 0;
        for (float[] q : queries) {
            Set<String> truth = new HashSet<>();
            exact.search(q, 10, 0).forEach(h -> truth.add(h.id()));
            recall += hnsw.search(q, 10, ef).stream().filter(h -> truth.contains(h.id())).count() / 10.0;
        }
        return recall / queries.length;
    }

    @Test
    void scoresAreCosineAndSorted() {
        HnswVectorIndex hnsw = new HnswVectorIndex(2, HnswVectorIndex.Params.DEFAULT, 16);
        hnsw.upsert("x", new float[] {1, 0}, "x-text");
        hnsw.upsert("y", new float[] {0, 1}, "y-text");
        List<Hit> hits = hnsw.search(new float[] {1, 0}, 2, 0);
        assertEquals("x", hits.get(0).id());
        assertEquals(1f, hits.get(0).score(), 1e-6);
        assertEquals(0f, hits.get(1).score(), 1e-6);
        assertEquals("x-text", hits.get(0).text());
    }

    @Test
    void reUpsertIsIdempotentAndUpdatesReplaceVectors() {
        HnswVectorIndex hnsw = new HnswVectorIndex(2, HnswVectorIndex.Params.DEFAULT, 16);
        hnsw.upsert("a", new float[] {1, 0}, "v1");
        hnsw.upsert("a", new float[] {1, 0}, "v1"); // duplicate delivery
        assertEquals(1, hnsw.size());
        hnsw.upsert("b", new float[] {0, 1}, "b");
        hnsw.upsert("a", new float[] {0, -1}, "v2"); // real update: new vector
        assertEquals(2, hnsw.size());
        Hit top = hnsw.search(new float[] {0, -1}, 1, 0).get(0);
        assertEquals("a", top.id());
        assertEquals("v2", top.text());

        assertTrue(hnsw.delete("a"));
        assertFalse(hnsw.delete("a"));
        assertEquals(List.of("b"), hnsw.search(new float[] {0, -1}, 5, 0).stream().map(Hit::id).toList());
    }

    @Test
    void saveAndLoadRoundTrip() throws Exception {
        Random r = new Random(1);
        HnswVectorIndex hnsw = new HnswVectorIndex(DIM, HnswVectorIndex.Params.DEFAULT, 16);
        float[][] data = clustered(r, 300);
        for (int i = 0; i < data.length; i++) hnsw.upsert("c" + i, data[i], "t" + i);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        hnsw.save(out);
        HnswVectorIndex loaded = HnswVectorIndex.load(new ByteArrayInputStream(out.toByteArray()), 64);
        assertEquals(300, loaded.size());
        assertEquals(hnsw.search(data[7], 5, 0), loaded.search(data[7], 5, 0));
        loaded.upsert("new", data[0], "n"); // still writable after load
        assertEquals(301, loaded.size());
    }
}
