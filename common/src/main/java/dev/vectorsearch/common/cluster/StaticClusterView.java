package dev.vectorsearch.common.cluster;

import dev.vectorsearch.common.ring.HashRing;
import java.util.LinkedHashMap;
import java.util.Map;

/** Fixed membership from configuration ("node-1=localhost:9101,node-2=localhost:9102"); every node counts as live. */
public final class StaticClusterView implements ClusterViewSource {
    private final ClusterView view;

    public StaticClusterView(Map<String, String> addresses) {
        this.view = new ClusterView(0, HashRing.of(addresses.keySet()), null, addresses);
    }

    public static StaticClusterView parse(String spec) {
        Map<String, String> addresses = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2 || kv[0].isBlank() || kv[1].isBlank()) throw new IllegalArgumentException("expected node=host:port in '" + spec + "'");
            addresses.put(kv[0].trim(), kv[1].trim());
        }
        return new StaticClusterView(addresses);
    }

    @Override
    public ClusterView view() {
        return view;
    }
}
