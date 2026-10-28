package dev.vectorsearch.shardnode.index;

import com.github.jelmerk.hnswlib.core.Item;
import java.io.ObjectStreamException;

/** What one HNSW node stores: the chunk id, its unit vector and the text returned with hits. */
public record ChunkItem(String id, float[] vector, String text) implements Item<String, float[]> {
    private static final long serialVersionUID = 1L;

    /**
     * How items are written into snapshots (hnswlib serializes each item with ObjectOutputStream.writeObject).
     * JAVA = the record itself (default Java serialization, the format before step 37); FP32 / FP16 = a compact
     * {@link DiskChunkItem} with the vector as 4 or 2 bytes per dimension. Memory always holds fp32. Snapshots of
     * any format load, because each file says which class it holds. Chosen per save call
     * ({@link HnswVectorIndex#save(java.io.OutputStream, DiskFormat)}), held in a thread-local while it runs.
     */
    public enum DiskFormat { JAVA, FP32, FP16 }

    static final ThreadLocal<DiskFormat> SAVING = ThreadLocal.withInitial(() -> DiskFormat.JAVA);

    @Override
    public int dimensions() {
        return vector.length;
    }

    /** Serialization hook: swap in the compact form when a compact format is configured. */
    private Object writeReplace() throws ObjectStreamException {
        DiskFormat f = SAVING.get();
        return f == DiskFormat.JAVA ? this : new DiskChunkItem(this, f == DiskFormat.FP16);
    }
}
