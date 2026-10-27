package dev.vectorsearch.coordinator.tools;

import dev.vectorsearch.proto.v1.ScoredChunk;
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
import java.util.concurrent.TimeUnit;

/**
 * Sends MS MARCO dev queries through a running coordinator and prints latency percentiles per pass:
 * {@code ./gradlew -q :coordinator:queryBench -Pargs="localhost:9100 200 10 2"} (target, queries, k, passes).
 * Pass 1 is mostly cache misses, pass 2 cache hits (same texts within the 60 s TTL).
 * {@code -Pargs="localhost:9100 show what is the manhattan project"} prints one result list.
 */
public final class QueryBench {
    public static void main(String[] args) throws Exception {
        String[] hp = args[0].split(":");
        ManagedChannel ch = NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().build();
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(ch);
        try {
            if (args[1].equals("show")) {
                String q = String.join(" ", Arrays.copyOfRange(args, 2, args.length));
                SearchResponse r = stub.withDeadlineAfter(2, TimeUnit.SECONDS).search(SearchRequest.newBuilder().setText(q).setK(5).build());
                System.out.println("query: " + q + (r.getPartial() ? "  (PARTIAL)" : ""));
                for (ScoredChunk h : r.getHitsList()) {
                    System.out.printf("  %.4f %-14s %s%n", h.getScore(), h.getChunkId(), h.getText().substring(0, Math.min(110, h.getText().length())));
                }
                return;
            }
            int n = Integer.parseInt(args[1]), k = Integer.parseInt(args[2]), passes = Integer.parseInt(args[3]);
            List<String> queries = new ArrayList<>();
            for (String line : Files.readAllLines(Path.of("data/msmarco/queries.dev.tsv"))) {
                if (queries.size() == n) break;
                queries.add(line.substring(line.indexOf('\t') + 1));
            }
            for (int pass = 1; pass <= passes; pass++) {
                long[] ns = new long[queries.size()];
                int failed = 0, partial = 0;
                Map<String, Integer> errors = new TreeMap<>();
                for (int i = 0; i < queries.size(); i++) {
                    long t0 = System.nanoTime();
                    try {
                        SearchResponse r = stub.withDeadlineAfter(2, TimeUnit.SECONDS).search(SearchRequest.newBuilder().setText(queries.get(i)).setK(k).build());
                        if (r.getPartial()) partial++;
                    } catch (StatusRuntimeException e) {
                        failed++;
                        errors.merge(e.getStatus().getCode().name(), 1, Integer::sum);
                    }
                    ns[i] = System.nanoTime() - t0;
                }
                Arrays.sort(ns);
                System.out.printf("pass %d: %d queries k=%d p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms failed=%d %s partial=%d%n", pass, ns.length, k,
                        ns[ns.length / 2] / 1e6, ns[(int) (ns.length * 0.95)] / 1e6, ns[(int) (ns.length * 0.99)] / 1e6, ns[ns.length - 1] / 1e6,
                        failed, errors, partial);
            }
        } finally {
            ch.shutdownNow();
        }
    }
}
