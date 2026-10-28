package dev.vectorsearch.shardnode.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Compact snapshot formats (BUILDLOG step 37): sizes, round trip, old snapshots still load, dedupe after fp16. */
class SnapshotFormatTest {
    static final int DIM = 384;

    static HnswVectorIndex build(float[][] data) {
        HnswVectorIndex idx = new HnswVectorIndex(DIM, new HnswVectorIndex.Params(16, 100, 64), 100);
        for (int i = 0; i < data.length; i++) idx.upsert("doc" + i + "#0", data[i], "passage text number " + i + " with some words in it");
        return idx;
    }

    static byte[] save(HnswVectorIndex idx, ChunkItem.DiskFormat f) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        idx.save(out, f);
        return out.toByteArray();
    }

    @Test
    void compactFormatsAreSmallerAndRoundTrip() throws Exception {
        float[][] data = new float[500][];
        Random r = new Random(7);
        for (int i = 0; i < data.length; i++) data[i] = BruteForceIndexTest.unit(r, DIM);
        HnswVectorIndex idx = build(data);
        byte[] java = save(idx, ChunkItem.DiskFormat.JAVA);
        byte[] fp32 = save(idx, ChunkItem.DiskFormat.FP32);
        byte[] fp16 = save(idx, ChunkItem.DiskFormat.FP16);
        System.out.printf("bytes/vector: java %.0f, fp32 %.0f, fp16 %.0f%n", java.length / 500.0, fp32.length / 500.0, fp16.length / 500.0);
        assertTrue(fp32.length < java.length * 1.02, "compact fp32 is about the same size as Java serialization");
        assertTrue(fp16.length < fp32.length * 0.65, "fp16 roughly halves the vector bytes");

        HnswVectorIndex l32 = HnswVectorIndex.load(new ByteArrayInputStream(fp32), 64);
        HnswVectorIndex l16 = HnswVectorIndex.load(new ByteArrayInputStream(fp16), 64);
        assertEquals(500, l32.size());
        assertEquals(500, l16.size());
        for (ChunkItem it : l32.items()) {
            int i = Integer.parseInt(it.id().substring(3, it.id().indexOf('#')));
            org.junit.jupiter.api.Assertions.assertArrayEquals(data[i], it.vector()); // fp32: exact
            assertEquals("passage text number " + i + " with some words in it", it.text());
        }
        for (ChunkItem it : l16.items()) {
            int i = Integer.parseInt(it.id().substring(3, it.id().indexOf('#')));
            for (int d = 0; d < DIM; d++) assertTrue(Math.abs(it.vector()[d] - data[i][d]) <= Math.abs(data[i][d]) / 2048f + 1e-7f);
        }
        // same top-1 for every stored vector, before and after an fp16 round trip
        for (int i = 0; i < data.length; i += 25) assertEquals(idx.search(data[i], 1, 64).get(0).id(), l16.search(data[i], 1, 64).get(0).id());
    }

    @Test
    void javaSnapshotStillLoadsWhenACompactFormatIsConfigured() throws Exception {
        float[][] data = new float[100][];
        Random r = new Random(9);
        for (int i = 0; i < data.length; i++) data[i] = BruteForceIndexTest.unit(r, DIM);
        byte[] legacy = save(build(data), ChunkItem.DiskFormat.JAVA);
        HnswVectorIndex loaded = HnswVectorIndex.load(new ByteArrayInputStream(legacy), 64);
        assertEquals(100, loaded.size());
        // and it is re-saved in the new format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        loaded.save(out, ChunkItem.DiskFormat.FP16);
        assertTrue(out.size() < legacy.length * 0.65);
    }

    @Test
    void redeliveryAfterFp16RestoreIsADuplicateNotAnUpdate() throws Exception {
        float[][] data = new float[50][];
        Random r = new Random(11);
        for (int i = 0; i < data.length; i++) data[i] = BruteForceIndexTest.unit(r, DIM);
        HnswVectorIndex restored = HnswVectorIndex.load(new ByteArrayInputStream(save(build(data), ChunkItem.DiskFormat.FP16)), 64);
        for (int i = 0; i < data.length; i++) restored.upsert("doc" + i + "#0", data[i], "passage text number " + i + " with some words in it");
        assertEquals(50, restored.duplicateUpserts());
        assertEquals(0, restored.replacedVectors());
        assertEquals(50, restored.size());
    }
}
