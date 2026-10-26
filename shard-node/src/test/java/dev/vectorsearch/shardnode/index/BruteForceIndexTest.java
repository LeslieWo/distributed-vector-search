package dev.vectorsearch.shardnode.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BruteForceIndexTest {

    static float[] unit(Random r, int dim) {
        float[] v = new float[dim];
        double n = 0;
        for (int i = 0; i < dim; i++) {
            v[i] = (float) r.nextGaussian();
            n += v[i] * v[i];
        }
        for (int i = 0; i < dim; i++) v[i] /= (float) Math.sqrt(n);
        return v;
    }

    static float dot(float[] a, float[] b) {
        float s = 0;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }

    @Test
    void topKMatchesSortingEverything() {
        Random r = new Random(7);
        int dim = 16;
        List<float[]> vecs = IntStream.range(0, 500).mapToObj(i -> unit(r, dim)).toList();
        BruteForceIndex index = new BruteForceIndex(dim, 4); // small capacity forces growth
        for (int i = 0; i < vecs.size(); i++) index.upsert("c" + i, vecs.get(i), "t" + i);
        float[] q = unit(r, dim);

        List<String> expected = IntStream.range(0, vecs.size()).boxed()
                .sorted(Comparator.comparingDouble(i -> -dot(q, vecs.get(i))))
                .limit(10).map(i -> "c" + i).toList();
        List<Hit> hits = index.search(q, 10, 0);
        assertEquals(expected, hits.stream().map(Hit::id).toList());
        assertEquals("t" + expected.get(0).substring(1), hits.get(0).text());
        assertTrue(hits.get(0).score() >= hits.get(9).score());
    }

    @Test
    void upsertSameIdOverwritesAndDeleteRemoves() {
        BruteForceIndex index = new BruteForceIndex(2);
        index.upsert("a", new float[] {1, 0}, "old");
        index.upsert("b", new float[] {0, 1}, "b");
        index.upsert("a", new float[] {0, -1}, "new");
        assertEquals(2, index.size());
        assertEquals("b", index.search(new float[] {0, 1}, 1, 0).get(0).id());
        assertEquals("new", index.search(new float[] {0, -1}, 1, 0).get(0).text());

        assertTrue(index.delete("a"));
        assertFalse(index.delete("a"));
        assertEquals(1, index.size());
        assertEquals(List.of("b"), index.search(new float[] {0, -1}, 5, 0).stream().map(Hit::id).toList());
    }
}
