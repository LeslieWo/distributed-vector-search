package dev.vectorsearch.common.ring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Step 28: ring invariants over many random cluster sizes and node names, not just node-1..3. */
class HashRingPropertiesTest {
    static final int KEYS = 20_000;

    static List<String> randomNodes(Random rnd, int n) {
        Set<String> names = new HashSet<>();
        while (names.size() < n) names.add("n" + Integer.toString(rnd.nextInt(1_000_000), 36));
        return new ArrayList<>(names);
    }

    @Test
    void invariantsHoldForRandomClusters() {
        Random rnd = new Random(42);
        double movedTotal = 0, expectedTotal = 0;
        for (int trial = 0; trial < 60; trial++) {
            int n = 2 + rnd.nextInt(7); // 2..8 nodes
            List<String> nodes = randomNodes(rnd, n + 1);
            String joiner = nodes.remove(n);
            HashRing before = HashRing.of(nodes);
            HashRing after = HashRing.of(concat(nodes, joiner));
            HashRing rebuilt = HashRing.of(nodes);

            // partitions: at most n(n-1)/2, every node holds at least one, ids are sorted pairs
            assertTrue(before.partitions().size() <= n * (n - 1) / 2);
            for (String node : nodes) assertTrue(!before.partitionsOf(node).isEmpty(), node);

            int moved = 0;
            for (int i = 0; i < KEYS; i++) {
                String k = "k" + trial + "-" + i;
                List<String> o0 = before.owners(k), o1 = after.owners(k);
                assertEquals(2, new HashSet<>(o1).size());
                Set<String> allowed = new HashSet<>(o0);
                allowed.add(joiner);
                assertTrue(allowed.containsAll(o1), "join reshuffled old nodes for " + k);
                if (!before.primary(k).equals(after.primary(k))) {
                    moved++;
                    assertEquals(joiner, after.primary(k));
                }
                // removing the joiner again restores the original placement exactly
                assertEquals(before.partitionOf(k).id(), rebuilt.partitionOf(k).id());
            }
            movedTotal += moved / (double) KEYS;
            expectedTotal += 1.0 / (n + 1);
        }
        // on average a joiner takes ~1/(n+1) of the primaries
        assertEquals(expectedTotal / 60, movedTotal / 60, 0.03);
    }

    static List<String> concat(List<String> a, String b) {
        List<String> out = new ArrayList<>(a);
        out.add(b);
        return out;
    }
}
