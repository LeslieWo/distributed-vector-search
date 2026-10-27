package dev.vectorsearch.common.cluster;

import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.common.ring.Partition;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.DeleteRequest;
import dev.vectorsearch.proto.v1.HealthRequest;
import dev.vectorsearch.proto.v1.HealthResponse;
import dev.vectorsearch.proto.v1.ScanRequest;
import dev.vectorsearch.proto.v1.ScanResponse;
import dev.vectorsearch.proto.v1.ShardServiceGrpc;
import dev.vectorsearch.proto.v1.UpsertRequest;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Adds/removes nodes by moving only the chunks whose partition changes (step 27).
 *
 * <ol>
 *   <li>Validate: every old partition has a live holder; nodes joining are live and empty.
 *   <li>Publish {@code vs:ring:pending}; wait until every writer acks it (from now on writes go to the owners
 *       under both rings, so nothing written during the copy can be missed).
 *   <li>Copy: for each old partition, ask one live holder to {@code Scan} it against the new ring; it streams
 *       only the chunks that move, which we upsert into their new partition on their new holders with
 *       {@code only_if_absent} (a newer live write that already landed there wins over our copy).
 *   <li>Commit the pending ring (atomic MULTI/EXEC); wait until every reader and writer uses it, plus a grace
 *       period for queries that started on the old ring.
 *   <li>Re-scan every old partition on every live old holder against the new ring and delete what no longer
 *       belongs there (the copied chunks, plus ones live writes added to the old partition during the copy).
 * </ol>
 * Crash-safe to re-run with the same target: the copy is idempotent and the old ring stays authoritative
 * until the commit.
 */
public final class Rebalancer {
    private final RingStore store;
    private final ChannelPool channels;
    private final Duration barrierTimeout;
    private final Duration settle;
    private final PrintStream out;

    /**
     * @param settle minimum wait after publishing a ring change before trusting the acks (covers a process that
     *               read the old view but has not registered its ack yet; > cluster poll interval) and grace after
     *               the commit for in-flight queries (> query deadline)
     */
    public Rebalancer(RingStore store, ChannelPool channels, Duration barrierTimeout, Duration settle, PrintStream out) {
        this.store = store;
        this.channels = channels;
        this.barrierTimeout = barrierTimeout;
        this.settle = settle;
        this.out = out;
    }

    public record Report(long version, int partitionsScanned, long chunksMoved, long copiesWritten, long copiesDeleted, long millis) {}

    public Report rebalance(List<String> targetNodes) throws InterruptedException {
        long t0 = System.nanoTime();
        RedisKeys.RingSpec current = store.ring();
        if (current == null) throw new IllegalStateException("no ring yet: run init first");
        HashRing oldRing = HashRing.of(current.nodes());
        HashRing newRing = HashRing.of(targetNodes);
        Map<String, String> live = store.liveNodes();

        // 1. validate before touching anything
        for (Partition p : oldRing.partitions()) {
            if (p.holders().stream().noneMatch(live::containsKey)) {
                throw new IllegalStateException("partition " + p.id() + " has no live holder; its data is unavailable");
            }
        }
        for (String n : newRing.nodes()) {
            if (!live.containsKey(n)) throw new IllegalStateException(n + " is in the target ring but not live");
            if (!oldRing.contains(n)) {
                HealthResponse h = stub(live.get(n)).health(HealthRequest.getDefaultInstance());
                long stored = h.getPartitionSizesMap().values().stream().mapToLong(Long::longValue).sum();
                if (stored > 0) throw new IllegalStateException(n + " joins the ring but already stores " + stored + " chunks; wipe it first");
            }
        }

        // 2. pending ring + writer barrier
        long version = store.beginRebalance(targetNodes);
        out.printf("pending ring v%d = %s (was v%d = %s)%n", version, newRing.nodes(), current.version(), oldRing.nodes());
        awaitViews(ViewAcker.WRITER, s -> s[1] == version, "writers to write to the pending ring");

        // 3. copy the chunks that move
        Map<String, List<String>> movedFrom = new LinkedHashMap<>(); // old partition -> moved chunk ids
        long copies = 0;
        for (Partition p : oldRing.partitions()) {
            String holder = p.holders().stream().filter(live::containsKey).findFirst().orElseThrow();
            List<String> moved = new ArrayList<>();
            Iterator<ScanResponse> scan = ShardServiceGrpc.newBlockingStub(channels.get(live.get(holder)))
                    .scan(ScanRequest.newBuilder().setPartition(p.id()).addAllRingNodes(newRing.nodes()).setVnodes(newRing.vnodes()).build());
            while (scan.hasNext()) {
                Map<String, UpsertRequest.Builder> perNode = new LinkedHashMap<>();
                for (ChunkVector c : scan.next().getChunksList()) {
                    moved.add(c.getChunkId());
                    for (String n : newRing.partitionOf(c.getChunkId()).holders()) {
                        perNode.computeIfAbsent(n, x -> UpsertRequest.newBuilder().setOnlyIfAbsent(true)).addChunks(c);
                    }
                }
                for (Map.Entry<String, UpsertRequest.Builder> e : perNode.entrySet()) {
                    stub(live.get(e.getKey())).upsert(e.getValue().build());
                    copies += e.getValue().getChunksCount();
                }
            }
            movedFrom.put(p.id(), moved);
            out.printf("  %s (scanned on %s): %d chunks move%n", p.id(), holder, moved.size());
        }

        // 4. switch everyone to the new ring
        store.commitRebalance();
        out.printf("ring v%d committed%n", version);
        awaitViews(ViewAcker.WRITER, s -> s[0] == version && s[1] == -1, "writers to drop the old ring");
        awaitViews(ViewAcker.READER, s -> s[0] == version, "readers to read the new ring");
        Thread.sleep(settle.toMillis()); // queries that started on the old ring finish

        // 5. remove what no longer belongs: on every live old holder, scan each old partition against the new ring
        //    again and delete what it returns. Re-scanning (instead of deleting the ids copied in step 3) also catches
        //    chunks that live writes put into the old partition after it was copied; those are in their new place
        //    too, because writes went to both rings until the commit.
        long deleted = 0;
        Map<String, String> liveNow = store.liveNodes();
        for (Partition p : oldRing.partitions()) {
            for (String h : p.holders()) {
                if (!liveNow.containsKey(h)) continue; // a removed dead node keeps its stale files; it is out of the ring
                List<String> stray = new ArrayList<>();
                ShardServiceGrpc.newBlockingStub(channels.get(liveNow.get(h)))
                        .scan(ScanRequest.newBuilder().setPartition(p.id()).addAllRingNodes(newRing.nodes()).setVnodes(newRing.vnodes()).build())
                        .forEachRemaining(m -> m.getChunksList().forEach(c -> stray.add(c.getChunkId())));
                for (int i = 0; i < stray.size(); i += 1000) {
                    deleted += stub(liveNow.get(h)).delete(DeleteRequest.newBuilder().setPartition(p.id())
                            .addAllChunkIds(stray.subList(i, Math.min(stray.size(), i + 1000))).build()).getDeleted();
                }
            }
        }
        long moved = movedFrom.values().stream().mapToLong(List::size).sum();
        Report r = new Report(version, oldRing.partitions().size(), moved, copies, deleted, (System.nanoTime() - t0) / 1_000_000);
        out.println(r);
        return r;
    }

    /** Re-copies every partition {@code node} holds from the other holder (e.g. after it lost its disk). */
    public long repair(String node) {
        HashRing ring = HashRing.of(store.ring().nodes());
        Map<String, String> live = store.liveNodes();
        if (!live.containsKey(node)) throw new IllegalStateException(node + " is not live");
        long copied = 0;
        for (Partition p : ring.partitionsOf(node)) {
            String source = p.other(node);
            if (!live.containsKey(source)) throw new IllegalStateException("cannot repair " + p.id() + ": " + source + " is not live");
            Iterator<ScanResponse> scan = stub(live.get(source)).scan(ScanRequest.newBuilder().setPartition(p.id()).build());
            while (scan.hasNext()) {
                UpsertRequest.Builder req = UpsertRequest.newBuilder().setOnlyIfAbsent(true).addAllChunks(scan.next().getChunksList());
                stub(live.get(node)).upsert(req.build());
                copied += req.getChunksCount();
            }
            out.printf("  %s copied from %s to %s%n", p.id(), source, node);
        }
        return copied;
    }

    /** partition -> (holder -> size) for every partition of the current ring, on live holders. */
    public Map<String, Map<String, Long>> partitionSizes() {
        HashRing ring = HashRing.of(store.ring().nodes());
        Map<String, String> live = store.liveNodes();
        Map<String, Map<String, Long>> sizes = new TreeMap<>();
        Map<String, HealthResponse> health = new TreeMap<>();
        for (String n : ring.nodes()) if (live.containsKey(n)) health.put(n, stub(live.get(n)).health(HealthRequest.getDefaultInstance()));
        for (Partition p : ring.partitions()) {
            Map<String, Long> perHolder = new TreeMap<>();
            for (String h : p.holders()) if (health.containsKey(h)) perHolder.put(h, health.get(h).getPartitionSizesOrDefault(p.id(), 0));
            sizes.put(p.id(), perHolder);
        }
        return sizes;
    }

    private void awaitViews(String role, Predicate<long[]> ok, String what) throws InterruptedException {
        Thread.sleep(settle.toMillis());
        long deadline = System.nanoTime() + barrierTimeout.toNanos();
        while (true) {
            Map<String, String> lagging = new TreeMap<>();
            store.views(role).forEach((id, s) -> {
                if (!ok.test(ViewAcker.parse(s))) lagging.put(id, s);
            });
            if (lagging.isEmpty()) return;
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("timed out waiting for " + what + ": " + lagging + " (re-run to resume)");
            }
            Thread.sleep(100);
        }
    }

    private ShardServiceGrpc.ShardServiceBlockingStub stub(String address) {
        return ShardServiceGrpc.newBlockingStub(channels.get(address)).withDeadlineAfter(60, TimeUnit.SECONDS);
    }
}
