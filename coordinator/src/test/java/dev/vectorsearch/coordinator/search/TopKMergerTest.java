package dev.vectorsearch.coordinator.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.vectorsearch.proto.v1.ScoredChunk;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class TopKMergerTest {
    static ScoredChunk c(String id, float score) {
        return ScoredChunk.newBuilder().setChunkId(id).setScore(score).build();
    }

    /** Reference: put everything in one list, sort, dedupe keeping the best, cut to k. */
    static List<ScoredChunk> fullSort(List<List<ScoredChunk>> lists, int k) {
        Map<String, ScoredChunk> best = new LinkedHashMap<>();
        lists.forEach(l -> l.forEach(h -> best.merge(h.getChunkId(), h, (a, b) -> TopKMerger.WORST_FIRST.compare(a, b) >= 0 ? a : b)));
        List<ScoredChunk> all = new ArrayList<>(best.values());
        all.sort(Collections.reverseOrder(TopKMerger.WORST_FIRST));
        return all.subList(0, Math.min(k, all.size()));
    }

    static List<ScoredChunk> sortedList(Random rnd, String prefix, int n) {
        List<ScoredChunk> l = new ArrayList<>();
        for (int i = 0; i < n; i++) l.add(c(prefix + i, rnd.nextInt(50) / 50f)); // many ties on purpose
        l.sort(Collections.reverseOrder(TopKMerger.WORST_FIRST));
        return l;
    }

    @Test
    void equalsAFullSortOnRandomInputsWithTies() {
        Random rnd = new Random(3);
        for (int trial = 0; trial < 500; trial++) {
            int k = 1 + rnd.nextInt(20);
            List<List<ScoredChunk>> lists = new ArrayList<>();
            int parts = 1 + rnd.nextInt(6);
            for (int p = 0; p < parts; p++) lists.add(sortedList(rnd, "p" + p + "-", rnd.nextInt(k + 3)));
            assertEquals(fullSort(lists, k), TopKMerger.merge(lists, k));
        }
    }

    @Test
    void keepsTheBestCopyOfADuplicateAndHandlesShortAndEmptyLists() {
        List<ScoredChunk> merged = TopKMerger.merge(List.of(
                List.of(c("a", 0.9f), c("dup", 0.5f)), List.of(), List.of(c("dup", 0.7f), c("b", 0.1f))), 10);
        assertEquals(List.of(c("a", 0.9f), c("dup", 0.7f), c("b", 0.1f)), merged);
        assertEquals(List.of(), TopKMerger.merge(List.of(List.of(), List.of()), 5));
    }

    @Test
    void bestFirstAcrossPartitions() {
        List<ScoredChunk> merged = TopKMerger.merge(List.of(
                List.of(c("x1", 0.95f), c("x2", 0.40f)), List.of(c("y1", 0.80f), c("y2", 0.79f)), List.of(c("z1", 0.99f))), 3);
        assertEquals(List.of("z1", "x1", "y1"), merged.stream().map(ScoredChunk::getChunkId).toList());
        merged.stream().map(ScoredChunk::getScore).reduce((a, b) -> {
            if (b > a) throw new AssertionError("not sorted");
            return b;
        });
    }
}
