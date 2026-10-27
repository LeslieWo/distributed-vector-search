package dev.vectorsearch.shardnode.store;

import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.index.ChunkItem;
import dev.vectorsearch.shardnode.index.Hit;
import dev.vectorsearch.shardnode.index.HnswVectorIndex;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * All HNSW indexes on this node, one per partition (partition = primary node id, see DESIGN.md).
 * A node stores its own partition plus the partitions it replicates.
 */
public class PartitionStore {
    private static final Pattern PARTITION_NAME = Pattern.compile("[A-Za-z0-9_.-]{1,64}");

    private final ShardNodeProperties props;
    protected final Map<String, HnswVectorIndex> partitions = new ConcurrentHashMap<>();

    public PartitionStore(ShardNodeProperties props) {
        this.props = props;
    }

    public record Entry(String chunkId, String partition, float[] vector, String text) {}

    public static boolean validPartitionName(String partition) {
        return partition != null && PARTITION_NAME.matcher(partition).matches();
    }

    /**
     * Inserts a batch in parallel (hnswlib-core supports concurrent adds; one HNSW insert is ~1.4 ms
     * single-threaded, BUILDLOG step 9). If the batch repeats a chunk id, the last occurrence wins.
     */
    public int upsert(List<Entry> entries) {
        Map<String, Entry> lastWins = new LinkedHashMap<>();
        for (Entry e : entries) lastWins.put(e.partition() + "/" + e.chunkId(), e);
        lastWins.values().parallelStream().forEach(e -> indexFor(e.partition()).upsert(e.chunkId(), e.vector(), e.text()));
        return entries.size();
    }

    public int delete(String partition, List<String> chunkIds) {
        HnswVectorIndex index = partitions.get(partition);
        if (index == null) return 0;
        int n = 0;
        for (String id : chunkIds) if (index.delete(id)) n++;
        return n;
    }

    /** Unknown partition = nothing stored for it yet = no hits (not an error). */
    public List<Hit> search(String partition, float[] query, int k, int ef) {
        HnswVectorIndex index = partitions.get(partition);
        return index == null ? List.of() : index.search(query, k, ef);
    }

    /** Live items of one partition (empty if unknown). */
    public List<ChunkItem> items(String partition) {
        HnswVectorIndex index = partitions.get(partition);
        return index == null ? List.of() : index.items();
    }

    public long size(String partition) {
        HnswVectorIndex index = partitions.get(partition);
        return index == null ? 0 : index.size();
    }

    public Map<String, Long> sizes() {
        Map<String, Long> out = new TreeMap<>();
        partitions.forEach((p, idx) -> out.put(p, (long) idx.size()));
        return out;
    }

    public long duplicateUpserts() {
        return partitions.values().stream().mapToLong(HnswVectorIndex::duplicateUpserts).sum();
    }

    public long replacedVectors() {
        return partitions.values().stream().mapToLong(HnswVectorIndex::replacedVectors).sum();
    }

    protected HnswVectorIndex indexFor(String partition) {
        return partitions.computeIfAbsent(partition, p -> newIndex());
    }

    protected HnswVectorIndex newIndex() {
        return new HnswVectorIndex(props.dimension(),
                new HnswVectorIndex.Params(props.hnswM(), props.hnswEfConstruction(), props.hnswEfSearch()),
                props.initialCapacity());
    }

    public ShardNodeProperties properties() {
        return props;
    }
}
