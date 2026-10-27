package dev.vectorsearch.common.cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis layout for cluster metadata (DESIGN.md "Redis"):
 * <ul>
 *   <li>{@code vs:nodes:<nodeId>} = "host:grpcPort", TTL 3 s, refreshed every 1 s by the node (liveness).
 *   <li>{@code vs:ring} = "version|node-1,node-2,node-3": who owns data (placement). Changed only by an operator.
 *   <li>{@code vs:ring:pending} = "version|nodes": the target ring while a rebalance copies data.
 *   <li>{@code vs:views:<role>:<id>} = ring versions a writer/reader has switched to, TTL 3 s (rebalance barrier).
 * </ul>
 */
public final class RedisKeys {
    public static final String NODE_PREFIX = "vs:nodes:";
    public static final String RING = "vs:ring";
    public static final String RING_PENDING = "vs:ring:pending";
    public static final String VIEW_PREFIX = "vs:views:";

    private RedisKeys() {}

    public static String node(String nodeId) {
        return NODE_PREFIX + nodeId;
    }

    /** "3|node-1,node-2,node-3" */
    public static String encodeRing(long version, List<String> nodes) {
        return version + "|" + String.join(",", nodes);
    }

    public record RingSpec(long version, List<String> nodes) {}

    public static RingSpec decodeRing(String value) {
        if (value == null) return null;
        int bar = value.indexOf('|');
        if (bar < 0) throw new IllegalArgumentException("bad ring value '" + value + "'");
        List<String> nodes = new ArrayList<>();
        for (String n : value.substring(bar + 1).split(",")) if (!n.isBlank()) nodes.add(n.trim());
        return new RingSpec(Long.parseLong(value.substring(0, bar)), nodes);
    }
}
