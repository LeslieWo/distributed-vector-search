package dev.vectorsearch.embedworker.it;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** Step 19: a shard outage longer than max.poll.interval.ms must not get the worker kicked out of its group. */
class BackpressureIT extends WorkerIT {

    @Test
    void longOutageDoesNotTriggerRebalanceAndLosesNothing() throws Exception {
        produce("d", 300);
        shard.down = true;
        try (ConfigurableApplicationContext w = startWorker("--worker.max-poll-interval=3s", "--worker.retry-backoff-max=500ms")) {
            MeterRegistry m = w.getBean(MeterRegistry.class);
            await().atMost(Duration.ofSeconds(30)).until(() -> shard.rejectedCalls.get() > 0);
            Thread.sleep(8_000); // outage lasts > 2x max.poll.interval
            produce("late", 50);  // keeps arriving during the outage
            shard.down = false;
            await().atMost(Duration.ofSeconds(60)).until(() -> committedTotal("embed-worker") == 350);
            assertEquals(0.0, m.counter("vs.worker.partitions.revoked").count(), "worker must stay in the group");
        }
        assertEquals(350, shard.chunkIds().size());
    }
}
