package dev.vectorsearch.coordinator.tools;

import dev.vectorsearch.common.cluster.RingStore;
import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.common.ring.Partition;
import dev.vectorsearch.embedding.MiniLmEmbedder;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.ScanRequest;
import dev.vectorsearch.proto.v1.ScoredChunk;
import dev.vectorsearch.proto.v1.SearchRequest;
import dev.vectorsearch.proto.v1.SearchResponse;
import dev.vectorsearch.proto.v1.SearchServiceGrpc;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * recall@k of the whole query path (coordinator: embedding + scatter-gather over HNSW partitions) against exact
 * brute force over every vector currently stored in the cluster (read back with Scan, one holder per partition):
 * {@code ./gradlew -q :coordinator:recallCheck -Pargs="localhost:9100 500 10 [efSearch-label]"}. Run with the query
 * cache off. Vectors are kept in one flat float[] (2M x 384 = 3 GB) and the exact top-k of each query is computed
 * in parallel, so this works at full scale.
 */
public final class RecallCheck {
    static final int DIM = MiniLmEmbedder.DIMENSION;

    public static void main(String[] args) throws Exception {
        String target = args[0];
        int n = Integer.parseInt(args[1]), k = Integer.parseInt(args[2]);
        List<String> ids = new ArrayList<>();
        float[][] flat = {new float[DIM * 2_100_000]}; // sized for the 2,015,656-chunk corpus; grows if needed
        long t0 = System.nanoTime();
        try (RingStore store = new RingStore("redis://localhost:6379")) {
            HashRing ring = HashRing.of(store.ring().nodes());
            Map<String, String> live = store.liveNodes();
            for (Partition p : ring.partitions()) {
                String holder = p.holders().stream().filter(live::containsKey).findFirst().orElseThrow();
                ManagedChannel ch = NettyChannelBuilder.forTarget(live.get(holder)).usePlaintext().maxInboundMessageSize(64 << 20).build();
                int before = ids.size();
                ShardServiceGrpc.newBlockingStub(ch).scan(ScanRequest.newBuilder().setPartition(p.id()).build()).forEachRemaining(m -> {
                    for (ChunkVector c : m.getChunksList()) {
                        int row = ids.size();
                        if ((row + 1) * DIM > flat[0].length) flat[0] = Arrays.copyOf(flat[0], flat[0].length / 4 * 5);
                        for (int i = 0; i < DIM; i++) flat[0][row * DIM + i] = c.getVector(i);
                        ids.add(c.getChunkId());
                    }
                });
                System.out.printf("scanned %s on %s: %d chunks%n", p.id(), holder, ids.size() - before);
                ch.shutdownNow();
            }
        }
        float[] vecs = flat[0];
        int rows = ids.size();
        System.out.printf("cluster holds %d chunks (%d distinct), scan took %.1f s%n", rows, new HashSet<>(ids).size(), (System.nanoTime() - t0) / 1e9);

        List<String> queries = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of("data/msmarco/queries.dev.tsv"))) {
            if (queries.size() == n) break;
            queries.add(line.substring(line.indexOf('\t') + 1));
        }
        float[][] qv;
        try (MiniLmEmbedder embedder = new MiniLmEmbedder(Path.of("data/models/all-MiniLM-L6-v2"))) {
            qv = embedder.embed(queries);
        }

        // exact top-k per query, queries in parallel (each scans all rows once)
        long t1 = System.nanoTime();
        List<Set<String>> exact = IntStream.range(0, queries.size()).parallel().mapToObj(q -> {
            float[] x = qv[q];
            PriorityQueue<float[]> heap = new PriorityQueue<>((a, b) -> Float.compare(a[0], b[0])); // {score, row}
            for (int r = 0; r < rows; r++) {
                float s = 0;
                int base = r * DIM;
                for (int i = 0; i < DIM; i++) s += x[i] * vecs[base + i];
                if (heap.size() < k) heap.add(new float[] {s, r});
                else if (s > heap.peek()[0]) {
                    heap.poll();
                    heap.add(new float[] {s, r});
                }
            }
            Set<String> top = new HashSet<>();
            heap.forEach(e -> top.add(ids.get((int) e[1])));
            return top;
        }).toList();
        System.out.printf("brute force top-%d for %d queries over %d vectors in %.1f s%n", k, queries.size(), rows, (System.nanoTime() - t1) / 1e9);

        String[] hp = target.split(":");
        ManagedChannel ch = NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().build();
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(ch);
        double recall = 0;
        int partial = 0, perfect = 0;
        try {
            for (int q = 0; q < queries.size(); q++) {
                SearchResponse r = stub.withDeadlineAfter(2, TimeUnit.SECONDS).search(SearchRequest.newBuilder().setText(queries.get(q)).setK(k).build());
                if (r.getPartial()) partial++;
                Set<String> truth = exact.get(q);
                long found = r.getHitsList().stream().map(ScoredChunk::getChunkId).filter(truth::contains).count();
                if (found == k) perfect++;
                recall += found / (double) k;
            }
        } finally {
            ch.shutdownNow();
        }
        System.out.printf("recall@%d over %d MS MARCO dev queries = %.4f (queries with all %d found: %d, partial responses: %d)%n",
                k, queries.size(), recall / queries.size(), k, perfect, partial);
    }
}
