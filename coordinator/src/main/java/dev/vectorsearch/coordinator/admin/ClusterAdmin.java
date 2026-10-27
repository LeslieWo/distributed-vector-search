package dev.vectorsearch.coordinator.admin;

import dev.vectorsearch.common.cluster.ChannelPool;
import dev.vectorsearch.common.cluster.Rebalancer;
import dev.vectorsearch.common.cluster.RingStore;
import dev.vectorsearch.common.cluster.ViewAcker;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Operator CLI for cluster metadata in Redis: {@code ./gradlew -q :coordinator:clusterAdmin -Pargs="..."}.
 * <ul>
 *   <li>{@code show} - ring, pending ring, live nodes, writer/reader views
 *   <li>{@code init node-1,node-2,node-3} - create ring v1 if none exists
 *   <li>{@code add node-4} / {@code remove node-2} / {@code set-ring node-1,node-3,node-4} - rebalance (moves only affected chunks)
 *   <li>{@code abort} - drop a pending rebalance (the current ring was never changed)
 *   <li>{@code check} - per partition, chunk count on each live holder (both holders must match)
 *   <li>{@code repair node-2} - re-copy every partition of node-2 from its other holders (e.g. after disk loss)
 * </ul>
 * Redis URI from {@code -Dredis=...} or env {@code VS_REDIS}, default redis://localhost:6379.
 */
public final class ClusterAdmin {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) usage();
        String redis = System.getProperty("redis", System.getenv().getOrDefault("VS_REDIS", "redis://localhost:6379"));
        try (RingStore store = new RingStore(redis);
             ChannelPool pool = new ChannelPool(a -> NettyChannelBuilder.forTarget(a).usePlaintext().maxInboundMessageSize(64 << 20).build())) {
            Rebalancer rb = new Rebalancer(store, pool, Duration.ofSeconds(60), Duration.ofSeconds(2), System.out);
            switch (args[0]) {
                case "show" -> {
                    System.out.println("ring    " + store.ring());
                    System.out.println("pending " + store.pending());
                    System.out.println("live    " + store.liveNodes());
                    System.out.println("writers " + store.views(ViewAcker.WRITER));
                    System.out.println("readers " + store.views(ViewAcker.READER));
                }
                case "init" -> {
                    List<String> nodes = Arrays.asList(arg(args).split(","));
                    System.out.println(store.initIfAbsent(nodes) ? "ring v1 = " + nodes : "ring already exists, unchanged: " + store.ring());
                }
                case "add" -> {
                    List<String> nodes = new ArrayList<>(store.ring().nodes());
                    nodes.add(arg(args));
                    rb.rebalance(nodes);
                }
                case "remove" -> {
                    List<String> nodes = new ArrayList<>(store.ring().nodes());
                    if (!nodes.remove(arg(args))) throw new IllegalArgumentException(args[1] + " is not in the ring");
                    rb.rebalance(nodes);
                }
                case "set-ring" -> rb.rebalance(Arrays.asList(arg(args).split(",")));
                case "abort" -> {
                    store.abortRebalance();
                    System.out.println("pending ring dropped; ring is " + store.ring());
                }
                case "check" -> {
                    long total = 0;
                    boolean ok = true;
                    for (Map.Entry<String, Map<String, Long>> e : rb.partitionSizes().entrySet()) {
                        boolean same = e.getValue().values().stream().distinct().count() <= 1;
                        ok &= same && !e.getValue().isEmpty();
                        total += e.getValue().values().stream().findFirst().orElse(0L);
                        System.out.println(e.getKey() + " " + e.getValue() + (same ? "" : "  <-- holders differ"));
                    }
                    System.out.println("chunks (one holder per partition): " + total + (ok ? "  OK" : "  MISMATCH"));
                }
                case "repair" -> System.out.println("copied " + rb.repair(arg(args)) + " chunks");
                default -> usage();
            }
        }
    }

    private static String arg(String[] args) {
        if (args.length < 2) usage();
        return args[1];
    }

    private static void usage() {
        System.err.println("usage: show | init n1,n2,n3 | add n | remove n | set-ring n1,n2 | abort | check | repair n");
        System.exit(2);
    }
}
