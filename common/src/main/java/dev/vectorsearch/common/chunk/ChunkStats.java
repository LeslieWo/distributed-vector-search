package dev.vectorsearch.common.chunk;

import dev.vectorsearch.common.dataset.Passage;
import dev.vectorsearch.common.dataset.TsvCorpus;
import java.nio.file.Path;
import java.util.stream.Stream;

/** Dev tool: chunks a whole TSV corpus and prints how many passages became how many chunks. */
public final class ChunkStats {
    public static void main(String[] args) {
        Path file = Path.of(args.length > 0 ? args[0] : "data/msmarco/collection.2M.tsv");
        Chunker chunker = new Chunker();
        long passages = 0, chunks = 0, multi = 0;
        int maxChunks = 0;
        long t0 = System.nanoTime();
        try (Stream<Passage> s = TsvCorpus.stream(file)) {
            for (Passage p : (Iterable<Passage>) s::iterator) {
                int n = chunker.chunk(p.docId(), p.text()).size();
                passages++;
                chunks += n;
                if (n > 1) multi++;
                maxChunks = Math.max(maxChunks, n);
            }
        }
        System.out.printf("passages=%d chunks=%d multiChunkPassages=%d maxChunksPerPassage=%d took=%.1fs%n",
                passages, chunks, multi, maxChunks, (System.nanoTime() - t0) / 1e9);
    }
}
