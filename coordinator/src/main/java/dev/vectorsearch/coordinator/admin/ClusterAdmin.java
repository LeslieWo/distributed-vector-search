package dev.vectorsearch.coordinator.admin;

import dev.vectorsearch.common.cluster.RingStore;
import java.util.Arrays;
import java.util.List;

/**
 * Operator CLI for cluster metadata in Redis: {@code ./gradlew -q :coordinator:clusterAdmin -Pargs="..."}.
 * <ul>
 *   <li>{@code show} - ring, pending ring, live nodes
 *   <li>{@code init node-1,node-2,node-3} - create ring v1 if none exists
 * </ul>
 * Redis URI from {@code -Dredis=...} or env {@code VS_REDIS}, default redis://localhost:6379.
 */
public final class ClusterAdmin {
    public static void main(String[] args) {
        if (args.length == 0) usage();
        String redis = System.getProperty("redis", System.getenv().getOrDefault("VS_REDIS", "redis://localhost:6379"));
        try (RingStore store = new RingStore(redis)) {
            switch (args[0]) {
                case "show" -> {
                    System.out.println("ring    " + store.ring());
                    System.out.println("pending " + store.pending());
                    System.out.println("live    " + store.liveNodes());
                }
                case "init" -> {
                    if (args.length < 2) usage();
                    List<String> nodes = Arrays.asList(args[1].split(","));
                    System.out.println(store.initIfAbsent(nodes) ? "ring v1 = " + nodes : "ring already exists, unchanged: " + store.ring());
                }
                default -> usage();
            }
        }
    }

    private static void usage() {
        System.err.println("usage: show | init node-1,node-2,node-3");
        System.exit(2);
    }
}
