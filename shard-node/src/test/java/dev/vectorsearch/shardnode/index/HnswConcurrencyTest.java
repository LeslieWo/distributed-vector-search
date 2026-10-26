package dev.vectorsearch.shardnode.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Writers trigger many capacity doublings while readers keep searching: nothing may throw or get lost. */
class HnswConcurrencyTest {

    @Test
    void concurrentUpsertsAndSearchesAcrossResizes() throws Exception {
        int dim = 16, writers = 4, perWriter = 2000;
        HnswVectorIndex index = new HnswVectorIndex(dim, new HnswVectorIndex.Params(8, 50, 32), 16);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicBoolean writing = new AtomicBoolean(true);
        AtomicLong searches = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        CountDownLatch writersDone = new CountDownLatch(writers);

        for (int w = 0; w < writers; w++) {
            int id = w;
            pool.submit(() -> {
                try {
                    Random r = new Random(id);
                    for (int i = 0; i < perWriter; i++) index.upsert("w" + id + "-" + i, BruteForceIndexTest.unit(r, dim), null);
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    writersDone.countDown();
                }
            });
        }
        for (int s = 0; s < 2; s++) {
            int seed = 100 + s;
            pool.submit(() -> {
                Random r = new Random(seed);
                try {
                    while (writing.get()) {
                        List<Hit> hits = index.search(BruteForceIndexTest.unit(r, dim), 5, 0);
                        assertTrue(hits.size() <= 5);
                        searches.incrementAndGet();
                    }
                } catch (Throwable t) {
                    errors.add(t);
                }
            });
        }
        assertTrue(writersDone.await(60, TimeUnit.SECONDS));
        writing.set(false);
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue(errors.isEmpty(), "errors: " + errors);
        assertEquals(writers * perWriter, index.size());
        assertTrue(searches.get() > 0);
    }
}
