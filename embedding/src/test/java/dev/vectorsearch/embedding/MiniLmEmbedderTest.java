package dev.vectorsearch.embedding;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Needs data/models/all-MiniLM-L6-v2 (scripts/fetch-data.sh); skipped when the model is absent. */
class MiniLmEmbedderTest {
    static MiniLmEmbedder embedder;

    @BeforeAll
    static void load() {
        Path dir = Path.of(System.getProperty("vs.modelDir", "data/models/all-MiniLM-L6-v2"));
        assumeTrue(Files.exists(dir.resolve("model.onnx")), "model not downloaded");
        embedder = new MiniLmEmbedder(dir);
    }

    @AfterAll
    static void close() {
        if (embedder != null) embedder.close();
    }

    @Test
    void vectorsAre384dAndUnitLength() {
        float[] v = embedder.embed("This is a test sentence.");
        assertEquals(384, v.length);
        assertEquals(1.0, Vectors.dot(v, v), 1e-4);
    }

    @Test
    void similarSentencesScoreHigherThanUnrelatedOnes() {
        float[][] v = embedder.embed(List.of(
                "How do I bake chocolate chip cookies?",
                "What is a good recipe for chocolate chip cookies?",
                "The stock market fell sharply on Tuesday."));
        float similar = Vectors.dot(v[0], v[1]);
        float unrelated = Vectors.dot(v[0], v[2]);
        System.out.printf("cos(similar)=%.4f cos(unrelated)=%.4f%n", similar, unrelated);
        assertTrue(similar > 0.7f, "similar pair " + similar);
        assertTrue(unrelated < 0.2f, "unrelated pair " + unrelated);
    }

    @Test
    void batchingAndPaddingDoNotChangeVectors() {
        String shortText = "Hi.";
        String longText = "A considerably longer sentence that forces the short one in the same batch to be padded.";
        float[] alone = embedder.embed(shortText);
        float[] inBatch = embedder.embed(List.of(longText, shortText))[1];
        assertArrayEquals(alone, inBatch, 1e-4f);
    }

    @Test
    void lengthSortedMicroBatchesKeepInputOrder() {
        List<String> texts = List.of(
                "a much longer text about the history of the Roman empire and its many emperors",
                "short",
                "medium length sentence here");
        float[][] batched = embedder.embed(texts);
        for (int i = 0; i < texts.size(); i++) assertArrayEquals(embedder.embed(texts.get(i)), batched[i], 1e-4f);
    }

    /**
     * reference_vectors.tsv was produced by sentence-transformers 6.2.0 / torch 2.14.1 (PyTorch, not ONNX)
     * with {@code encode(..., normalize_embeddings=True)}; see BUILDLOG step 13.
     */
    @Test
    void matchesSentenceTransformersReference() throws Exception {
        List<String> lines = Files.readAllLines(Path.of(getClass().getResource("/reference_vectors.tsv").toURI()));
        assertEquals(4, lines.size());
        for (String line : lines) {
            String[] parts = line.split("\t");
            String[] nums = parts[1].split(" ");
            float[] ref = new float[nums.length];
            for (int i = 0; i < nums.length; i++) ref[i] = Float.parseFloat(nums[i]);
            float[] ours = embedder.embed(parts[0]);
            float maxAbsDiff = 0f;
            for (int i = 0; i < ref.length; i++) maxAbsDiff = Math.max(maxAbsDiff, Math.abs(ref[i] - ours[i]));
            float cos = Vectors.dot(ref, ours);
            System.out.printf("ref check cos=%.7f maxAbsDiff=%.2e  %s%n", cos, maxAbsDiff, parts[0]);
            assertTrue(cos > 0.9999f, "cosine to reference " + cos);
            assertTrue(maxAbsDiff < 1e-3f, "max abs diff " + maxAbsDiff);
        }
    }
}
