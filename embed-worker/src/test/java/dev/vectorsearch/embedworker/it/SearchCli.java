package dev.vectorsearch.embedworker.it;

import dev.vectorsearch.embedding.MiniLmEmbedder;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.ShardSearchRequest;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;

/**
 * Dev tool: embeds a query text with the real model and calls ShardService.Search on a running node.
 * Args: host:port partition k "query text..." (no query = print Health only).
 */
public final class SearchCli {
    public static void main(String[] args) {
        String[] hp = args[0].split(":");
        ManagedChannel ch = NettyChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1])).usePlaintext().build();
        try {
            var stub = ShardServiceGrpc.newBlockingStub(ch);
            System.out.println("health: " + stub.health(HealthRequest.getDefaultInstance()).getPartitionSizesMap());
            if (args.length < 4) return;
            String query = String.join(" ", java.util.Arrays.copyOfRange(args, 3, args.length));
            try (MiniLmEmbedder e = new MiniLmEmbedder(Path.of("data/models/all-MiniLM-L6-v2"))) {
                ShardSearchRequest.Builder req = ShardSearchRequest.newBuilder().setPartition(args[1]).setK(Integer.parseInt(args[2]));
                for (float x : e.embed(query)) req.addVector(x);
                System.out.println("query: " + query);
                stub.search(req.build()).getHitsList().forEach(h -> System.out.printf("  %.4f %s %s%n", h.getScore(), h.getChunkId(),
                        h.getText().substring(0, Math.min(90, h.getText().length()))));
            }
        } finally {
            ch.shutdownNow();
        }
    }
}
