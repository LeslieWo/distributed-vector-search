package dev.vectorsearch.common.ring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class HashRingTest {
    static final List<String> THREE = List.of("node-1", "node-2", "node-3");
    static final int KEYS = 1_000_000;

    @Test
    void ownersAreTwoDistinctNodesAndPartitionIsTheirSortedPair() {
        HashRing ring = HashRing.of(THREE);
        for (int i = 0; i < 10_000; i++) {
            String k = "doc" + i + "#0";
            List<String> owners = ring.owners(k);
            assertEquals(2, owners.size());
            assertNotEquals(owners.get(0), owners.get(1));
            assertEquals(ring.primary(k), owners.get(0));
            Partition p = ring.partitionOf(k);
            assertEquals(new HashSet<>(owners), new HashSet<>(p.holders()));
            assertEquals(String.join(".", p.holders()), p.id());
        }
        // same input, same answer, independent of the order nodes were given in
        HashRing again = HashRing.of(List.of("node-3", "node-1", "node-2"));
        for (int i = 0; i < 1000; i++) assertEquals(ring.owners(i + "#0"), again.owners(i + "#0"));
    }

    @Test
    void threeNodesGiveThreePartitionsEachNodeHoldsTwo() {
        HashRing ring = HashRing.of(THREE);
        assertEquals(List.of("node-1.node-2", "node-1.node-3", "node-2.node-3"), ring.partitions().stream().map(Partition::id).toList());
        for (String n : THREE) assertEquals(2, ring.partitionsOf(n).size());
    }

    /** 1M keys, 3 nodes x 128 vnodes. Measured: max/min 1.283 for these node names (BUILDLOG step 21). */
    @Test
    void keysSpreadEvenlyWith128Vnodes() {
        HashRing ring = HashRing.of(THREE);
        Map<String, Integer> primary = new TreeMap<>();
        for (int i = 0; i < KEYS; i++) primary.merge(ring.primary(i + "#0"), 1, Integer::sum);
        assertTrue(RingStats.ratio(primary, THREE) < 1.35, "primary max/min " + primary);
        // key counts follow the analytic arc shares (keys hash uniformly)
        ring.primaryShare().forEach((n, share) -> assertEquals(share, primary.get(n) / (double) KEYS, 0.005, n));
        assertEquals(1.0, ring.primaryShare().values().stream().mapToDouble(d -> d).sum(), 1e-9);
        // a single point per node is badly skewed: that is why vnodes exist
        Map<String, Integer> one = new TreeMap<>();
        HashRing single = HashRing.of(THREE, 1);
        for (int i = 0; i < 100_000; i++) one.merge(single.primary(i + "#0"), 1, Integer::sum);
        assertTrue(RingStats.ratio(one, THREE) > 3, "1 vnode " + one);
    }

    @Test
    void addingAFourthNodeMovesAboutAQuarterOfKeysAllToTheNewNode() {
        HashRing before = HashRing.of(THREE);
        HashRing after = HashRing.of(List.of("node-1", "node-2", "node-3", "node-4"));
        int moved = 0;
        for (int i = 0; i < KEYS; i++) {
            String k = i + "#0";
            String p0 = before.primary(k), p1 = after.primary(k);
            if (!p0.equals(p1)) {
                moved++;
                assertEquals("node-4", p1, "a key moved between two old nodes: " + k);
            }
            // owners can only change by node-4 stepping in; nothing shuffles among the old nodes
            Set<String> allowed = new HashSet<>(before.owners(k));
            allowed.add("node-4");
            assertTrue(allowed.containsAll(after.owners(k)), k);
        }
        double fraction = moved / (double) KEYS;
        assertTrue(fraction > 0.20 && fraction < 0.30, "moved " + fraction); // measured 0.2421
    }

    @Test
    void removingANodeOnlyMovesItsOwnKeys() {
        HashRing before = HashRing.of(List.of("node-1", "node-2", "node-3", "node-4"));
        HashRing after = HashRing.of(THREE);
        for (int i = 0; i < 200_000; i++) {
            String k = i + "#0";
            if (!before.primary(k).equals("node-4")) assertEquals(before.primary(k), after.primary(k));
        }
    }

    @Test
    void singleNodeRingHasOneOwner() {
        HashRing ring = HashRing.of(List.of("solo"));
        assertEquals(List.of("solo"), ring.owners("x#0"));
        assertEquals("solo", ring.partitionOf("x#0").id());
        assertEquals(1, ring.partitions().size());
    }

    @Test
    void rejectsBadNodeIds() {
        assertThrows(IllegalArgumentException.class, () -> HashRing.of(List.of()));
        assertThrows(IllegalArgumentException.class, () -> HashRing.of(List.of("a.b"))); // '.' separates partition ids
        assertThrows(IllegalArgumentException.class, () -> HashRing.of(List.of("../x")));
    }
}
