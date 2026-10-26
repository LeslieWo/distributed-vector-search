package dev.vectorsearch.embedding;

/** Small float-vector helpers shared by the embedder, the index and the benchmarks. */
public final class Vectors {
    private Vectors() {}

    public static float dot(float[] a, float[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("dimension mismatch " + a.length + " vs " + b.length);
        float s = 0f;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }

    /** Normalizes in place to unit length; a zero vector is left as is. */
    public static float[] normalize(float[] v) {
        double n = 0;
        for (float x : v) n += (double) x * x;
        n = Math.sqrt(n);
        if (n > 0) for (int i = 0; i < v.length; i++) v[i] = (float) (v[i] / n);
        return v;
    }
}
