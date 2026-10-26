package dev.vectorsearch.shardnode.bench;

import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardSearchResponse;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Talks to a running shard-node over gRPC using the bench subset.
 * Args: {@code load|query|health} [host:port=localhost:9101] [N=100000] [Q=1000] [partition=node-1] [ef=0]
 */
public final class ShardClient {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "health";
        String target = args.length > 1 ? args[1] : "localhost:9101";
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 100_000;
        int q = args.length > 3 ? Integer.parseInt(args[3]) : 1000;
        String partition = args.length > 4 ? args[4] : "node-1";
        int ef = args.length > 5 ? Integer.parseInt(args[5]) : 0;
        String[] hp = target.split(":");
        ManagedChannel ch = NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().maxInboundMessageSize(32 << 20).build();
        var stub = ShardServiceGrpc.newBlockingStub(ch);
        try {
            switch (mode) {
                case "health" -> System.out.println(stub.health(HealthRequest.getDefaultInstance()));
                case "load" -> load(stub, BenchData.load(n, q, false), partition);
                case "query" -> query(stub, BenchData.load(n, q, true), partition, ef);
                default -> throw new IllegalArgumentException(mode);
            }
        } finally {
            ch.shutdownNow();
        }
    }

    static void load(ShardServiceGrpc.ShardServiceBlockingStub stub, BenchData d, String partition) {
        long t0 = System.nanoTime();
        int batch = 256;
        for (int i = 0; i < d.ids().size(); i += batch) {
            UpsertRequest.Builder req = UpsertRequest.newBuilder();
            for (int j = i; j < Math.min(i + batch, d.ids().size()); j++) {
                ChunkVector.Builder c = ChunkVector.newBuilder().setChunkId(d.ids().get(j)).setPartition(partition).setText(d.texts().get(j));
                for (float x : d.vectors()[j]) c.addVector(x);
                req.addChunks(c);
            }
            stub.upsert(req.build());
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("loaded %d vectors in %.1fs (%.0f/s) over gRPC, batch %d%n", d.ids().size(), secs, d.ids().size() / secs, batch);
        System.out.println(stub.health(HealthRequest.getDefaultInstance()));
    }

    static void query(ShardServiceGrpc.ShardServiceBlockingStub stub, BenchData d, String partition, int ef) {
        Map<String, Integer> slotOf = new HashMap<>();
        for (int i = 0; i < d.ids().size(); i++) slotOf.put(d.ids().get(i), i);
        int q = d.queries().length;
        int[][] approx = new int[q][];
        long[] nanos = new long[q];
        for (int round = 0; round < 2; round++) { // round 0 = warm-up
            for (int i = 0; i < q; i++) {
                ShardSearchRequest.Builder req = ShardSearchRequest.newBuilder().setPartition(partition).setK(10).setEf(ef);
                for (float x : d.queries()[i]) req.addVector(x);
                long t0 = System.nanoTime();
                ShardSearchResponse resp = stub.search(req.build());
                nanos[i] = System.nanoTime() - t0;
                approx[i] = resp.getHitsList().stream().mapToInt(h -> slotOf.getOrDefault(h.getChunkId(), -1)).toArray();
                if (round == 1 && i == 0) {
                    System.out.println("query 0 top hit: " + resp.getHits(0).getChunkId() + " score=" + resp.getHits(0).getScore()
                            + " text=" + resp.getHits(0).getText().substring(0, Math.min(80, resp.getHits(0).getText().length())));
                }
            }
        }
        Arrays.sort(nanos);
        System.out.printf("gRPC search %d queries: recall@10=%.4f p50=%.3fms p99=%.3fms (single client thread, incl. network + serialization)%n",
                q, BenchData.recallAtK(d.groundTruth(), approx, 10), nanos[q / 2] / 1e6, nanos[(int) (q * 0.99)] / 1e6);
    }
}
