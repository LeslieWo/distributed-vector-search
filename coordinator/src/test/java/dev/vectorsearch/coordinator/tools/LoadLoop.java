package dev.vectorsearch.coordinator.tools;

import dev.vectorsearch.proto.v1.SearchRequest;
import dev.vectorsearch.proto.v1.SearchResponse;
import dev.vectorsearch.proto.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Closed-loop query load for failover experiments (k6 comes in Phase 5):
 * {@code ./gradlew -q :coordinator:loadLoop -Pargs="localhost:9100 4 40"} (target, threads, seconds).
 * Each thread sends MS MARCO dev queries back to back with a 1 s client deadline. Prints one line per second
 * (ok / failed / partial / latency percentiles) and a summary.
 */
public final class LoadLoop {
    record Sample(int second, long nanos, boolean ok, boolean partial, String error) {}

    public static void main(String[] args) throws Exception {
        String[] hp = args[0].split(":");
        int threads = Integer.parseInt(args[1]), seconds = Integer.parseInt(args[2]);
        List<String> queries = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of("data/msmarco/queries.dev.tsv"))) {
            if (queries.size() == 2000) break;
            queries.add(line.substring(line.indexOf('\t') + 1));
        }
        ManagedChannel ch = NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().build();
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(ch);
        List<Sample> samples = java.util.Collections.synchronizedList(new ArrayList<>());
        long start = System.nanoTime(), end = start + TimeUnit.SECONDS.toNanos(seconds);
        System.out.println("load start at epoch ms " + System.currentTimeMillis());
        AtomicInteger next = new AtomicInteger();
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            ts[t] = Thread.ofPlatform().start(() -> {
                while (System.nanoTime() < end) {
                    String q = queries.get(Math.floorMod(next.getAndIncrement(), queries.size()));
                    long t0 = System.nanoTime();
                    int sec = (int) ((t0 - start) / 1_000_000_000L);
                    try {
                        SearchResponse r = stub.withDeadlineAfter(1, TimeUnit.SECONDS).search(SearchRequest.newBuilder().setText(q).setK(10).build());
                        samples.add(new Sample(sec, System.nanoTime() - t0, true, r.getPartial(), null));
                    } catch (StatusRuntimeException e) {
                        samples.add(new Sample(sec, System.nanoTime() - t0, false, false, e.getStatus().getCode().name()));
                    }
                }
            });
        }
        for (Thread t : ts) t.join();
        ch.shutdownNow();

        Map<Integer, List<Sample>> bySecond = new TreeMap<>();
        for (Sample s : samples) bySecond.computeIfAbsent(s.second, x -> new ArrayList<>()).add(s);
        System.out.println("sec  queries  failed  partial   p50ms   p99ms   maxms");
        bySecond.forEach((sec, list) -> System.out.printf("%3d %8d %7d %8d %7.2f %7.2f %7.2f%n", sec, list.size(),
                list.stream().filter(s -> !s.ok).count(), list.stream().filter(s -> s.partial).count(),
                pct(list, 0.5), pct(list, 0.99), pct(list, 1.0)));
        Map<String, Integer> errors = new ConcurrentHashMap<>();
        samples.stream().filter(s -> !s.ok).forEach(s -> errors.merge(s.error, 1, Integer::sum));
        System.out.printf("TOTAL queries=%d failed=%d %s partial=%d p50=%.2fms p99=%.2fms max=%.2fms qps=%.1f%n", samples.size(),
                samples.stream().filter(s -> !s.ok).count(), errors, samples.stream().filter(s -> s.partial).count(),
                pct(samples, 0.5), pct(samples, 0.99), pct(samples, 1.0), samples.size() / (double) seconds);
    }

    static double pct(List<Sample> list, double p) {
        long[] ns = list.stream().mapToLong(Sample::nanos).toArray();
        Arrays.sort(ns);
        return ns[Math.min(ns.length - 1, (int) (ns.length * p))] / 1e6;
    }
}
