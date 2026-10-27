package dev.vectorsearch.common.ring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Consistent-hash ring with virtual nodes (DESIGN.md "Data placement"). Immutable; build a new one when
 * membership changes.
 *
 * <ul>
 *   <li>Each physical node gets {@code vnodes} points on a 64-bit ring at {@code murmur3(nodeId + "#" + i)}.
 *   <li>A key hashes to {@code murmur3(key)}; the first point clockwise (>= the key hash, wrapping) is its
 *       <b>primary</b> P. Walking further clockwise, the first point owned by a different physical node is
 *       its <b>replica</b> R (replication factor 2).
 *   <li>The key's {@link Partition} is the unordered pair {P, R}.
 * </ul>
 */
public final class HashRing {
    public static final int DEFAULT_VNODES = 128;
    private static final Pattern NODE_ID = Pattern.compile("[A-Za-z0-9_-]{1,32}");

    private final List<String> nodes;
    private final int vnodes;
    private final long[] points;      // sorted ascending (signed order; any fixed order works for a ring)
    private final String[] primaryAt; // owner of points[i]
    private final Partition[] partitionAt; // {owner of points[i], next distinct owner clockwise}
    private final List<Partition> partitions;

    private HashRing(Collection<String> nodeIds, int vnodes) {
        if (vnodes <= 0) throw new IllegalArgumentException("vnodes must be > 0");
        TreeSet<String> sorted = new TreeSet<>(nodeIds);
        if (sorted.isEmpty()) throw new IllegalArgumentException("ring needs at least one node");
        for (String n : sorted) {
            if (!NODE_ID.matcher(n).matches()) throw new IllegalArgumentException("bad node id '" + n + "' (allowed: [A-Za-z0-9_-]{1,32})");
        }
        this.nodes = List.copyOf(sorted);
        this.vnodes = vnodes;

        // TreeMap: sorted by position. On the (astronomically unlikely) collision the first node in id order keeps it.
        TreeMap<Long, String> ring = new TreeMap<>();
        for (String n : nodes) {
            for (int i = 0; i < vnodes; i++) ring.putIfAbsent(Murmur3.hash64(n + "#" + i), n);
        }
        int size = ring.size();
        points = new long[size];
        primaryAt = new String[size];
        int i = 0;
        for (Map.Entry<Long, String> e : ring.entrySet()) {
            points[i] = e.getKey();
            primaryAt[i] = e.getValue();
            i++;
        }
        partitionAt = new Partition[size];
        Map<String, Partition> distinct = new LinkedHashMap<>();
        for (i = 0; i < size; i++) {
            String p = primaryAt[i];
            String r = null;
            for (int j = 1; j < size; j++) {
                String candidate = primaryAt[(i + j) % size];
                if (!candidate.equals(p)) {
                    r = candidate;
                    break;
                }
            }
            Partition part = Partition.of(p, r);
            partitionAt[i] = distinct.computeIfAbsent(part.id(), id -> part); // share one instance per id
        }
        TreeMap<String, Partition> byId = new TreeMap<>();
        distinct.values().forEach(pt -> byId.put(pt.id(), pt));
        partitions = List.copyOf(byId.values());
    }

    public static HashRing of(Collection<String> nodeIds) {
        return new HashRing(nodeIds, DEFAULT_VNODES);
    }

    public static HashRing of(Collection<String> nodeIds, int vnodes) {
        return new HashRing(nodeIds, vnodes);
    }

    /** Index of the first point clockwise from the key's hash. */
    private int pointFor(String key) {
        long h = Murmur3.hash64(key);
        int idx = Arrays.binarySearch(points, h);
        if (idx < 0) idx = -idx - 1; // insertion point = first point > h
        return idx == points.length ? 0 : idx; // wrap around past the largest point
    }

    public String primary(String key) {
        return primaryAt[pointFor(key)];
    }

    /** [primary, replica] (just [primary] on a single-node ring). */
    public List<String> owners(String key) {
        int i = pointFor(key);
        String p = primaryAt[i];
        Partition part = partitionAt[i];
        return part.holders().size() == 1 ? List.of(p) : List.of(p, part.other(p));
    }

    public Partition partitionOf(String key) {
        return partitionAt[pointFor(key)];
    }

    /** Every partition that owns at least one arc, sorted by id. A query must search each exactly once. */
    public List<Partition> partitions() {
        return partitions;
    }

    /** Partitions stored on {@code node}. */
    public List<Partition> partitionsOf(String node) {
        List<Partition> out = new ArrayList<>();
        for (Partition p : partitions) if (p.heldBy(node)) out.add(p);
        return out;
    }

    public List<String> nodes() {
        return nodes;
    }

    public int vnodes() {
        return vnodes;
    }

    public boolean contains(String node) {
        return nodes.contains(node);
    }

    /**
     * Fraction of the hash space for which each node is primary (sum of the arcs ending at its points).
     * With uniform key hashes this is the expected share of keys.
     */
    public Map<String, Double> primaryShare() {
        Map<String, Double> share = new TreeMap<>();
        for (String n : nodes) share.put(n, 0.0);
        double full = Math.pow(2, 64);
        for (int i = 0; i < points.length; i++) {
            long prev = points[(i - 1 + points.length) % points.length];
            // arc (prev, points[i]]; unsigned subtraction wraps correctly, a single point owns the full ring
            double arc = points.length == 1 ? full : unsigned(points[i] - prev);
            share.merge(primaryAt[i], arc / full, Double::sum);
        }
        return share;
    }

    private static double unsigned(long x) {
        return x >= 0 ? x : (x >>> 1) * 2.0 + (x & 1);
    }

    @Override
    public String toString() {
        return "HashRing" + nodes + "x" + vnodes;
    }
}
