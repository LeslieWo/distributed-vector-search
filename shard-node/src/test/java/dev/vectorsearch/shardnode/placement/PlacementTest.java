package dev.vectorsearch.shardnode.placement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.vectorsearch.common.ring.HashRing;
import dev.vectorsearch.common.ring.Partition;
import dev.vectorsearch.common.ring.Placement;
import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.index.ChunkItem;
import dev.vectorsearch.shardnode.store.PartitionStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** 3 nodes x 2 replicas on real partition stores: every chunk on exactly its 2 holders, a query plan covers each once. */
class PlacementTest {
    static final int DIM = 8;

    @Test
    void everyChunkLivesOnBothHoldersAndOnePlanCoversEachChunkOnce() {
        List<String> nodes = List.of("node-1", "node-2", "node-3");
        HashRing ring = HashRing.of(nodes);
        Map<String, PartitionStore> stores = new TreeMap<>();
        for (String n : nodes) stores.put(n, new PartitionStore(new ShardNodeProperties(n, 0, DIM, 16, 100, 32, 16, 100, 2, null, null, 0)));

        Random rnd = new Random(7);
        List<PartitionStore.Entry> chunks = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            float[] v = new float[DIM];
            for (int d = 0; d < DIM; d++) v[d] = (float) rnd.nextGaussian();
            chunks.add(new PartitionStore.Entry(i + "#0", null, v, "t" + i));
        }
        Map<String, Map<String, List<PartitionStore.Entry>>> plan = Placement.byNode(ring, chunks, PartitionStore.Entry::chunkId);
        plan.forEach((node, byPartition) -> byPartition.forEach((partition, list) -> stores.get(node).upsert(
                list.stream().map(e -> new PartitionStore.Entry(e.chunkId(), partition, e.vector(), e.text())).toList())));

        // each node stores exactly the partitions the ring says, and both holders of a partition agree
        for (String n : nodes) {
            assertEquals(ring.partitionsOf(n).stream().map(Partition::id).collect(java.util.stream.Collectors.toSet()), stores.get(n).sizes().keySet());
        }
        Map<String, Integer> copies = new HashMap<>();
        long covered = 0;
        Set<String> seen = new HashSet<>();
        for (Partition p : ring.partitions()) {
            long a = stores.get(p.holders().get(0)).size(p.id()), b = stores.get(p.holders().get(1)).size(p.id());
            assertEquals(a, b, p.id());
            covered += a;
            // read the whole partition on one holder: ids must not repeat across partitions
            for (ChunkItem item : stores.get(p.holders().get(0)).items(p.id())) {
                assertEquals(true, seen.add(item.id()), "chunk in two partitions: " + item.id());
            }
            for (String h : p.holders()) copies.merge(h, (int) a, Integer::sum);
        }
        assertEquals(3000, covered);
        assertEquals(3000, seen.size());
        System.out.println("partition sizes " + new TreeMap<>(stores.get("node-1").sizes()) + " " + stores.get("node-2").sizes()
                + "; copies per node " + new TreeMap<>(copies));
    }
}
