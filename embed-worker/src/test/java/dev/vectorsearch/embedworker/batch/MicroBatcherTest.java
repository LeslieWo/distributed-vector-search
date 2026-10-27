package dev.vectorsearch.embedworker.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MicroBatcherTest {
    final AtomicLong now = new AtomicLong(1000);
    final MicroBatcher<Integer> b = new MicroBatcher<>(3, 50, now::get);

    @Test
    void flushesWhenFull() {
        b.add(1);
        b.add(2);
        assertFalse(b.ready());
        b.add(3);
        assertTrue(b.ready());
        assertEquals(List.of(1, 2, 3), b.drain());
        assertEquals(0, b.size());
    }

    @Test
    void flushesWhenOldestItemWaitedLongEnough() {
        b.add(1);
        now.addAndGet(30);
        b.add(2);
        assertFalse(b.ready());
        assertEquals(20, b.millisUntilDue(500)); // 50 ms window measured from the first item
        now.addAndGet(20);
        assertTrue(b.ready());
        assertEquals(List.of(1, 2), b.drain());
    }

    @Test
    void emptyIsNeverReadyAndWaitsIdleTimeout() {
        now.addAndGet(10_000);
        assertFalse(b.ready());
        assertEquals(500, b.millisUntilDue(500));
    }

    @Test
    void drainTakesAtMostMaxItemsAndKeepsTheRest() {
        for (int i = 0; i < 5; i++) b.add(i);
        assertEquals(List.of(0, 1, 2), b.drain());
        assertEquals(2, b.size());
        assertFalse(b.ready()); // the rest got a fresh window
    }
}
