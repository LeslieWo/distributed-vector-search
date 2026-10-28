package dev.vectorsearch.shardnode.bench;

import dev.vectorsearch.shardnode.index.ChunkItem;
import dev.vectorsearch.shardnode.index.Hit;
import dev.vectorsearch.shardnode.index.HnswVectorIndex;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Snapshot formats on real data (BUILDLOG step 37): {@code ./gradlew -q :shard-node:snapshotFormatCheck}.
 * Builds HNSW (M=16, efC=200) over the 100k embedded MS MARCO passages, saves it in each format, reloads,
 * and compares bytes/vector and recall@10 (ef=80, 1000 dev queries vs exact ground truth) before and after.
 */
public final class SnapshotFormatCheck {
    public static void main(String[] args) throws Exception {
        BenchData d = BenchData.load(100_000, 1000, true);
        int n = d.vectors().length;
        HnswVectorIndex idx = new HnswVectorIndex(384, HnswVectorIndex.Params.DEFAULT, n);
        IntStream.range(0, n).parallel().forEach(i -> idx.upsert(d.ids().get(i), d.vectors()[i], d.texts().get(i)));
        Map<String, Integer> row = new HashMap<>();
        for (int i = 0; i < n; i++) row.put(d.ids().get(i), i);
        System.out.printf("built %d vectors; recall@10 in memory = %.4f%n", n, recall(idx, d, row));
        List<Hit> before0 = idx.search(d.queries()[0], 10, 80);
        for (ChunkItem.DiskFormat f : ChunkItem.DiskFormat.values()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long t0 = System.nanoTime();
            idx.save(out, f);
            long save = (System.nanoTime() - t0) / 1_000_000;
            t0 = System.nanoTime();
            HnswVectorIndex loaded = HnswVectorIndex.load(new ByteArrayInputStream(out.toByteArray()), 80);
            long load = (System.nanoTime() - t0) / 1_000_000;
            int same = 0;
            for (int q = 0; q < d.queries().length; q++) {
                if (ids(idx.search(d.queries()[q], 10, 80)).equals(ids(loaded.search(d.queries()[q], 10, 80)))) same++;
            }
            System.out.printf("%-4s %,d bytes = %.0f bytes/vector, save %d ms, load %d ms, size %d, recall@10 after reload = %.4f, identical top-10 lists %d/1000%n",
                    f, out.size(), out.size() / (double) n, save, load, loaded.size(), recall(loaded, d, row), same);
        }
        System.out.println("query 0 top-3 in memory: " + before0.subList(0, 3).stream().map(h -> h.id() + String.format("=%.4f", h.score())).toList());
    }

    static List<String> ids(List<Hit> hits) {
        return hits.stream().map(Hit::id).toList();
    }

    static double recall(HnswVectorIndex idx, BenchData d, Map<String, Integer> row) {
        int[][] approx = new int[d.queries().length][];
        for (int q = 0; q < approx.length; q++) approx[q] = idx.search(d.queries()[q], 10, 80).stream().mapToInt(h -> row.get(h.id())).toArray();
        return BenchData.recallAtK(d.groundTruth(), approx, 10);
    }
}
