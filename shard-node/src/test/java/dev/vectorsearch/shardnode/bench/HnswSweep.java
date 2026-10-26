package dev.vectorsearch.shardnode.bench;

import dev.vectorsearch.shardnode.index.Hit;
import dev.vectorsearch.shardnode.index.HnswVectorIndex;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * recall@10 vs single-thread query latency over efSearch, for several M values.
 * Args: [N=100000] [Q=1000] [M list=8,16,32] [efConstruction=200] [ef list=10,20,40,80,160,320]
 */
public final class HnswSweep {
    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        int q = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
        int[] ms = parse(args.length > 2 ? args[2] : "8,16,32");
        int efc = args.length > 3 ? Integer.parseInt(args[3]) : 200;
        int[] efs = parse(args.length > 4 ? args[4] : "10,20,40,80,160,320");
        BenchData d = BenchData.load(n, q, true);
        Map<String, Integer> slotOf = new HashMap<>();
        for (int i = 0; i < n; i++) slotOf.put(d.ids().get(i), i);

        System.out.println("| M | efConstruction | build s | efSearch | recall@10 | p50 ms | p99 ms | mean ms |");
        System.out.println("|---|---|---|---|---|---|---|---|");
        for (int m : ms) {
            HnswVectorIndex index = new HnswVectorIndex(d.vectors()[0].length, new HnswVectorIndex.Params(m, efc, 64), n);
            long t0 = System.nanoTime();
            IntStream.range(0, n).parallel().forEach(i -> index.upsert(d.ids().get(i), d.vectors()[i], null));
            double buildSecs = (System.nanoTime() - t0) / 1e9;
            for (int ef : efs) {
                for (int i = 0; i < Math.min(200, q); i++) index.search(d.queries()[i], 10, ef); // warm-up
                int[][] approx = new int[q][];
                long[] nanos = new long[q];
                for (int i = 0; i < q; i++) {
                    long s = System.nanoTime();
                    List<Hit> hits = index.search(d.queries()[i], 10, ef);
                    nanos[i] = System.nanoTime() - s;
                    approx[i] = hits.stream().mapToInt(h -> slotOf.get(h.id())).toArray();
                }
                Arrays.sort(nanos);
                System.out.printf("| %d | %d | %.1f | %d | %.4f | %.3f | %.3f | %.3f |%n", m, efc, buildSecs, ef,
                        BenchData.recallAtK(d.groundTruth(), approx, 10),
                        nanos[q / 2] / 1e6, nanos[(int) (q * 0.99)] / 1e6, Arrays.stream(nanos).average().orElse(0) / 1e6);
            }
        }
    }

    static int[] parse(String csv) {
        return Arrays.stream(csv.split(",")).mapToInt(Integer::parseInt).toArray();
    }
}
