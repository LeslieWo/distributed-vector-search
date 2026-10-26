package dev.vectorsearch.shardnode.bench;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** The embedded subset written by {@link EmbedSubset}: passages, queries and (optionally) ground truth. */
record BenchData(List<String> ids, List<String> texts, float[][] vectors, float[][] queries, int[][] groundTruth) {
    static final Path DIR = Path.of("data/vectors");

    static Path passages(int n) { return DIR.resolve("passages-" + n + ".f32"); }
    static Path passageIds(int n) { return DIR.resolve("passages-" + n + ".ids"); }
    static Path passageTexts(int n) { return DIR.resolve("passages-" + n + ".txt"); }
    static Path queries(int q) { return DIR.resolve("queries-" + q + ".f32"); }
    static Path queryTexts(int q) { return DIR.resolve("queries-" + q + ".txt"); }
    static Path groundTruth(int n, int q) { return DIR.resolve("gt-" + n + "-" + q + ".i32"); }

    static BenchData load(int n, int q, boolean withGroundTruth) throws IOException {
        return new BenchData(
                VectorFile.readLines(passageIds(n)),
                VectorFile.readLines(passageTexts(n)),
                VectorFile.readFloats(passages(n)),
                VectorFile.readFloats(queries(q)),
                withGroundTruth ? VectorFile.readInts(groundTruth(n, q)) : null);
    }

    /** recall@k = |approx top-k ∩ exact top-k| / k, averaged over queries. */
    static double recallAtK(int[][] exact, int[][] approx, int k) {
        double sum = 0;
        for (int i = 0; i < approx.length; i++) {
            java.util.Set<Integer> truth = new java.util.HashSet<>();
            for (int j = 0; j < k; j++) truth.add(exact[i][j]);
            int found = 0;
            for (int j = 0; j < Math.min(k, approx[i].length); j++) if (truth.contains(approx[i][j])) found++;
            sum += (double) found / k;
        }
        return sum / approx.length;
    }
}
