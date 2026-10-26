package dev.vectorsearch.shardnode.bench;

import dev.vectorsearch.shardnode.index.BruteForceIndex;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Exact top-100 for every query via brute force (the "answer key" for recall), plus the
 * single-thread latency of brute force itself as the baseline. Args: [N=100000] [Q=1000].
 */
public final class GroundTruth {
    static final int DEPTH = 100;

    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        int q = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
        BenchData d = BenchData.load(n, q, false);
        BruteForceIndex index = new BruteForceIndex(d.vectors()[0].length, n);
        for (int i = 0; i < n; i++) index.upsert(d.ids().get(i), d.vectors()[i], null);

        // Baseline latency, single thread, after a warm-up pass.
        for (int i = 0; i < 50; i++) index.topK(d.queries()[i], 10);
        long[] nanos = new long[Math.min(200, q)];
        for (int i = 0; i < nanos.length; i++) {
            long t0 = System.nanoTime();
            index.topK(d.queries()[i], 10);
            nanos[i] = System.nanoTime() - t0;
        }
        Arrays.sort(nanos);
        System.out.printf("brute force n=%d k=10 single-thread: p50=%.2fms p99=%.2fms mean=%.2fms (%d queries)%n",
                n, nanos[nanos.length / 2] / 1e6, nanos[(int) (nanos.length * 0.99)] / 1e6,
                Arrays.stream(nanos).average().orElse(0) / 1e6, nanos.length);

        long t0 = System.nanoTime();
        int[][] gt = IntStream.range(0, q).parallel().mapToObj(i -> index.topK(d.queries()[i], DEPTH)).toArray(int[][]::new);
        System.out.printf("ground truth top-%d for %d queries in %.1fs (parallel)%n", DEPTH, q, (System.nanoTime() - t0) / 1e9);
        VectorFile.writeInts(BenchData.groundTruth(n, q), gt);
        System.out.println("query 0: " + d.ids().get(gt[0][0]) + " score=" + dot(d.queries()[0], d.vectors()[gt[0][0]])
                + " text=" + d.texts().get(gt[0][0]).substring(0, Math.min(100, d.texts().get(gt[0][0]).length())));
    }

    static float dot(float[] a, float[] b) {
        float s = 0;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }
}
