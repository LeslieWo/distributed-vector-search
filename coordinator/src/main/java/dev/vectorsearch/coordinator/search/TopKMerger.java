package dev.vectorsearch.coordinator.search;

import dev.vectorsearch.proto.v1.ScoredChunk;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Merges per-partition top-k lists into the global top-k with a bounded min-heap.
 *
 * <p>The heap holds the best k seen so far with the <em>worst</em> of them on top, so deciding whether a new
 * hit belongs is one comparison against the top, and inserting costs O(log k). Total O(P*k*log k) for P
 * partitions, memory O(k), instead of sorting all P*k hits. Because every shard returns its hits sorted best
 * first, we stop reading a partition's list at the first hit that cannot beat the heap's worst.
 */
public final class TopKMerger {
    /** Worst first: lower score, then (for equal scores) the larger chunk id, so results are deterministic. */
    static final Comparator<ScoredChunk> WORST_FIRST =
            Comparator.comparingDouble(ScoredChunk::getScore).thenComparing(ScoredChunk::getChunkId, Comparator.reverseOrder());

    private TopKMerger() {}

    public static List<ScoredChunk> merge(List<List<ScoredChunk>> perPartition, int k) {
        PriorityQueue<ScoredChunk> heap = new PriorityQueue<>(k + 1, WORST_FIRST);
        Map<String, ScoredChunk> inHeap = new HashMap<>(); // a chunk lives in one partition; guard anyway
        for (List<ScoredChunk> hits : perPartition) {
            for (ScoredChunk h : hits) {
                if (heap.size() == k && WORST_FIRST.compare(h, heap.peek()) <= 0) break; // rest of this list is worse
                ScoredChunk dup = inHeap.get(h.getChunkId());
                if (dup != null) {
                    if (WORST_FIRST.compare(h, dup) <= 0) continue;
                    heap.remove(dup);
                }
                heap.add(h);
                inHeap.put(h.getChunkId(), h);
                if (heap.size() > k) inHeap.remove(heap.poll().getChunkId());
            }
        }
        List<ScoredChunk> out = new ArrayList<>(heap);
        out.sort(Collections.reverseOrder(WORST_FIRST)); // best first
        return out;
    }
}
