package dev.vectorsearch.shardnode.bench;

import dev.vectorsearch.common.chunk.Chunk;
import dev.vectorsearch.common.chunk.Chunker;
import dev.vectorsearch.common.dataset.Passage;
import dev.vectorsearch.common.dataset.TsvCorpus;
import dev.vectorsearch.embedding.MiniLmEmbedder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/** Embeds the first N chunks of the corpus and the first Q dev queries. Args: [N=100000] [Q=1000]. */
public final class EmbedSubset {
    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        int q = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
        Chunker chunker = new Chunker();
        List<Chunk> chunks = new ArrayList<>(n);
        try (Stream<Passage> s = TsvCorpus.stream(Path.of("data/msmarco/collection.2M.tsv"))) {
            Iterator<Passage> it = s.iterator();
            while (it.hasNext() && chunks.size() < n) {
                Passage p = it.next();
                for (Chunk c : chunker.chunk(p.docId(), p.text())) if (chunks.size() < n) chunks.add(c);
            }
        }
        List<String> queries;
        try (Stream<Passage> s = TsvCorpus.stream(Path.of("data/msmarco/queries.dev.tsv"))) {
            queries = s.limit(q).map(Passage::text).toList();
        }
        try (MiniLmEmbedder e = new MiniLmEmbedder(Path.of("data/models/all-MiniLM-L6-v2"))) {
            long t0 = System.nanoTime();
            float[][] vecs = new float[chunks.size()][];
            int batch = 64;
            for (int i = 0; i < chunks.size(); i += batch) {
                List<String> texts = chunks.subList(i, Math.min(i + batch, chunks.size())).stream().map(Chunk::text).toList();
                float[][] out = e.embed(texts);
                System.arraycopy(out, 0, vecs, i, out.length);
                if ((i / batch) % 200 == 0) {
                    double secs = (System.nanoTime() - t0) / 1e9;
                    System.out.printf("%d/%d chunks, %.0f/s%n", i + out.length, chunks.size(), (i + out.length) / secs);
                }
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("embedded %d chunks in %.1fs (%.0f chunks/s)%n", chunks.size(), secs, chunks.size() / secs);
            float[][] qv = e.embed(queries);

            VectorFile.writeFloats(BenchData.passages(n), vecs);
            Files.write(BenchData.passageIds(n), chunks.stream().map(c -> c.id().value()).toList());
            Files.write(BenchData.passageTexts(n), chunks.stream().map(c -> c.text().replace('\n', ' ')).toList());
            VectorFile.writeFloats(BenchData.queries(q), qv);
            Files.write(BenchData.queryTexts(q), queries);
            System.out.println("wrote " + BenchData.DIR.toAbsolutePath());
        }
    }
}
