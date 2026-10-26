package dev.vectorsearch.embedding;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Dev tool ({@code ./gradlew :embedding:embedBench}): sentences/sec at batch 1/32/64 on real
 * MS MARCO passages, plus a word-piece length check for 128-word chunks.
 */
public final class EmbedBench {
    private static void report(String label, MiniLmEmbedder e, List<String> texts, int batch) {
        long t0 = System.nanoTime();
        for (int i = 0; i < texts.size(); i += batch) e.embed(texts.subList(i, Math.min(i + batch, texts.size())));
        double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("%s sentences=%d seconds=%.2f sentences/sec=%.0f%n", label, texts.size(), secs, texts.size() / secs);
    }

    public static void main(String[] args) throws Exception {
        Path modelDir = Path.of("data/models/all-MiniLM-L6-v2");
        Path corpus = Path.of("data/msmarco/collection.2M.tsv");
        List<String> texts = new ArrayList<>();
        List<String> longTexts = new ArrayList<>();
        try (BufferedReader r = Files.newBufferedReader(corpus)) {
            String line;
            while ((line = r.readLine()) != null && (texts.size() < 4096 || longTexts.size() < 2000)) {
                String text = line.substring(line.indexOf('\t') + 1);
                if (texts.size() < 4096) texts.add(text);
                String[] words = text.split("\\s+");
                if (words.length >= 128 && longTexts.size() < 2000) {
                    longTexts.add(String.join(" ", List.of(words).subList(0, 128)));
                }
            }
        }
        // Part 1: rows per onnxruntime call = call size (micro-batch == batch), unsorted real passages.
        try (MiniLmEmbedder e = new MiniLmEmbedder(modelDir, MiniLmEmbedder.DEFAULT_MAX_TOKENS, 64, 0)) {
            e.embed(texts.subList(0, 256)); // warm-up: JIT + onnxruntime graph init
            for (int batch : new int[] {1, 32, 64}) {
                int n = batch == 1 ? 512 : texts.size();
                report("batch=" + batch + " microBatch=" + batch, e, texts.subList(0, n), batch);
            }
        }
        // Part 2: callers hand over 64 texts, the embedder sorts them by length and splits into micro-batches.
        for (int micro : new int[] {4, 8, 16}) {
            try (MiniLmEmbedder e = new MiniLmEmbedder(modelDir, MiniLmEmbedder.DEFAULT_MAX_TOKENS, micro, 0)) {
                e.embed(texts.subList(0, 256));
                report("batch=64 microBatch=" + micro + " (length-sorted)", e, texts, 64);
            }
        }
        try (MiniLmEmbedder e = new MiniLmEmbedder(modelDir)) {
            int over = 0, max = 0;
            long sum = 0;
            for (String t : longTexts) {
                // count without truncation would need another tokenizer instance; >=256 means it was truncated
                int c = e.tokenCount(t);
                sum += c;
                max = Math.max(max, c);
                if (c >= MiniLmEmbedder.DEFAULT_MAX_TOKENS) over++;
            }
            System.out.printf("128-word chunks=%d avgWordPieces=%.1f max=%d hitting256Limit=%d%n",
                    longTexts.size(), (double) sum / longTexts.size(), max, over);
        }
    }
}
