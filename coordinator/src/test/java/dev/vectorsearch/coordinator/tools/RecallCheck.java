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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * recall@k of the whole query path (coordinator: embedding + scatter-gather over HNSW partitions) against exact
 * brute force over every vector currently stored in the cluster (read back with Scan, one holder per partition):
 * {@code ./gradlew -q :coordinator:recallCheck -Pargs="localhost:9100 500 10"}. Run with the query cache off.
 */
public final class RecallCheck {
    public static void main(String[] args) throws Exception {
        String target = args[0];
        int n = Integer.parseInt(args[1]), k = Integer.parseInt(args[2]);
        List<String> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        try (RingStore store = new RingStore("redis://localhost:6379")) {
            HashRing ring = HashRing.of(store.ring().nodes());
            Map<String, String> live = store.liveNodes();
            for (Partition p : ring.partitions()) {
                String holder = p.holders().stream().filter(live::containsKey).findFirst().orElseThrow();
                ManagedChannel ch = NettyChannelBuilder.forTarget(live.get(holder)).usePlaintext().maxInboundMessageSize(64 << 20).build();
                int before = ids.size();
                ShardServiceGrpc.newBlockingStub(ch).scan(ScanRequest.newBuilder().setPartition(p.id()).build()).forEachRemaining(m -> {
                    for (ChunkVector c : m.getChunksList()) {
                        float[] v = new float[c.getVectorCount()];
                        for (int i = 0; i < v.length; i++) v[i] = c.getVector(i);
                        ids.add(c.getChunkId());
                        vecs.add(v);
                    }
                });
                System.out.printf("scanned %s on %s: %d chunks%n", p.id(), holder, ids.size() - before);
                ch.shutdownNow();
            }
        }
        System.out.println("cluster holds " + ids.size() + " chunks (" + new HashSet<>(ids).size() + " distinct)");

        List<String> queries = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of("data/msmarco/queries.dev.tsv"))) {
            if (queries.size() == n) break;
            queries.add(line.substring(line.indexOf('\t') + 1));
        }
        String[] hp = target.split(":");
        ManagedChannel ch = NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().build();
        SearchServiceGrpc.SearchServiceBlockingStub stub = SearchServiceGrpc.newBlockingStub(ch);
        double recall = 0;
        int partial = 0;
        try (MiniLmEmbedder embedder = new MiniLmEmbedder(Path.of("data/models/all-MiniLM-L6-v2"))) {
            for (String q : queries) {
                float[] qv = embedder.embed(q);
                PriorityQueue<Integer> heap = new PriorityQueue<>((a, b) -> Double.compare(dot(qv, vecs.get(a)), dot(qv, vecs.get(b))));
                for (int i = 0; i < vecs.size(); i++) {
                    heap.add(i);
                    if (heap.size() > k) heap.poll();
                }
                Set<String> exact = new HashSet<>();
                heap.forEach(i -> exact.add(ids.get(i)));
                SearchResponse r = stub.withDeadlineAfter(2, TimeUnit.SECONDS).search(SearchRequest.newBuilder().setText(q).setK(k).build());
                if (r.getPartial()) partial++;
                recall += r.getHitsList().stream().map(ScoredChunk::getChunkId).filter(exact::contains).count() / (double) k;
            }
        } finally {
            ch.shutdownNow();
        }
        System.out.printf("recall@%d over %d MS MARCO dev queries = %.4f (partial responses: %d)%n", k, queries.size(), recall / queries.size(), partial);
    }

    static double dot(float[] a, float[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }
}
