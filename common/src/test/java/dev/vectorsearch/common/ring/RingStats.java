package dev.vectorsearch.common.ring;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Prints ring balance and movement numbers for BUILDLOG step 21: {@code ./gradlew -q :common:ringStats}.
 * Keys are MS MARCO-style chunk ids "{pid}#0".
 */
public final class RingStats {
    public static void main(String[] args) {
        int keys = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        List<String> three = List.of("node-1", "node-2", "node-3");
        System.out.printf("keys=%d nodes=%s%n", keys, three);
        System.out.println("vnodes | primary keys per node | max/min | partitions (pair -> keys) | stored copies max/min");
        for (int v : new int[] {1, 8, 32, 64, 128, 256, 512}) {
            HashRing ring = HashRing.of(three, v);
            Map<String, Integer> primary = new TreeMap<>();
            Map<String, Integer> parts = new TreeMap<>();
            Map<String, Integer> copies = new TreeMap<>();
            for (int i = 0; i < keys; i++) {
                String k = i + "#0";
                primary.merge(ring.primary(k), 1, Integer::sum);
                Partition p = ring.partitionOf(k);
                parts.merge(p.id(), 1, Integer::sum);
                for (String h : p.holders()) copies.merge(h, 1, Integer::sum);
            }
            System.out.printf("%4d | %s | %.3f | %s | %.3f%n", v, primary, ratio(primary, three), parts, ratio(copies, three));
        }

        // One set of node names is one random draw; show the spread over 50 differently named 3-node clusters.
        System.out.println("vnodes | primary-share max/min over 50 clusters: median, p90, worst");
        for (int v : new int[] {1, 8, 32, 64, 128, 256, 512}) {
            double[] ratios = new double[50];
            for (int c = 0; c < ratios.length; c++) {
                Map<String, Double> share = HashRing.of(List.of("c" + c + "-a", "c" + c + "-b", "c" + c + "-c"), v).primaryShare();
                double max = share.values().stream().mapToDouble(d -> d).max().orElseThrow();
                double min = share.values().stream().mapToDouble(d -> d).min().orElseThrow();
                ratios[c] = max / min;
            }
            java.util.Arrays.sort(ratios);
            System.out.printf("%4d | %.3f %.3f %.3f%n", v, ratios[25], ratios[45], ratios[49]);
        }

        HashRing before = HashRing.of(three);
        HashRing after = HashRing.of(List.of("node-1", "node-2", "node-3", "node-4"));
        int primaryMoved = 0, ownerSetChanged = 0, copiesToNew = 0, primaryMovedNotToNew = 0;
        Map<String, Integer> newPrimary = new HashMap<>();
        for (int i = 0; i < keys; i++) {
            String k = i + "#0";
            String p0 = before.primary(k), p1 = after.primary(k);
            if (!p0.equals(p1)) {
                primaryMoved++;
                if (!p1.equals("node-4")) primaryMovedNotToNew++;
            }
            if (!before.partitionOf(k).equals(after.partitionOf(k))) ownerSetChanged++;
            if (after.partitionOf(k).heldBy("node-4")) copiesToNew++;
            newPrimary.merge(p1, 1, Integer::sum);
        }
        System.out.printf("add node-4 (128 vnodes): primary moved %.4f of keys (%d, of which %d not to node-4); "
                        + "owner set changed %.4f; copies node-4 must receive %d = %.4f of all %d copies; primaries after %s%n",
                primaryMoved / (double) keys, primaryMoved, primaryMovedNotToNew, ownerSetChanged / (double) keys,
                copiesToNew, copiesToNew / (2.0 * keys), 2L * keys, new TreeMap<>(newPrimary));
    }

    static double ratio(Map<String, Integer> counts, List<String> nodes) {
        int max = 0, min = Integer.MAX_VALUE;
        for (String n : nodes) {
            int c = counts.getOrDefault(n, 0);
            max = Math.max(max, c);
            min = Math.min(min, c);
        }
        return min == 0 ? Double.POSITIVE_INFINITY : max / (double) min;
    }
}
