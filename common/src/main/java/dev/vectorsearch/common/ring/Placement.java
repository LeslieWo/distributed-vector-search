package dev.vectorsearch.common.ring;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/** Where each item must be stored: on both holders of its partition, inside that partition's index. */
public final class Placement {
    private Placement() {}

    /** node -> partition id -> items. Each item appears under exactly the holders of {@code ring.partitionOf(key)}. */
    public static <T> Map<String, Map<String, List<T>>> byNode(HashRing ring, Collection<T> items, Function<T, String> key) {
        Map<String, Map<String, List<T>>> out = new TreeMap<>();
        for (T item : items) {
            Partition p = ring.partitionOf(key.apply(item));
            for (String node : p.holders()) {
                out.computeIfAbsent(node, n -> new LinkedHashMap<>())
                        .computeIfAbsent(p.id(), id -> new java.util.ArrayList<>()).add(item);
            }
        }
        return out;
    }
}
