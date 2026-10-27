package dev.vectorsearch.embedding;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

/**
 * Deterministic fake embedder: same text -> same unit vector, no model needed. Texts sharing words
 * get somewhat similar vectors (sum of per-word random vectors). For tests and pipeline load tests only.
 */
public final class HashingEmbedder implements Embedder {
    private final int dim;

    public HashingEmbedder(int dim) {
        this.dim = dim;
    }

    @Override
    public int dimension() {
        return dim;
    }

    @Override
    public float[][] embed(List<String> texts) {
        float[][] out = new float[texts.size()][];
        for (int i = 0; i < out.length; i++) {
            float[] v = new float[dim];
            for (String word : texts.get(i).toLowerCase().split("\\W+")) {
                if (word.isEmpty()) continue;
                Random r = new Random(fnv(word));
                for (int d = 0; d < dim; d++) v[d] += (float) r.nextGaussian();
            }
            if (allZero(v)) v[0] = 1f;
            out[i] = Vectors.normalize(v);
        }
        return out;
    }

    private static boolean allZero(float[] v) {
        for (float x : v) if (x != 0f) return false;
        return true;
    }

    private static long fnv(String s) {
        long h = 0xcbf29ce484222325L;
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            h ^= b;
            h *= 0x100000001b3L;
        }
        return h;
    }
}
