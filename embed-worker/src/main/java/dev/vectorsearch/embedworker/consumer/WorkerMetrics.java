package dev.vectorsearch.embedworker.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/** Counters/timers of the consume -> embed -> write -> commit loop. */
public final class WorkerMetrics {
    final Counter records;
    final Counter written;
    final Counter retries;
    final Counter dlq;
    final Counter commits;
    final Timer embed;
    final Timer write;
    final DistributionSummary batchSize;

    public WorkerMetrics(MeterRegistry r) {
        records = Counter.builder("vs.worker.records").description("records taken from raw-chunks").register(r);
        written = Counter.builder("vs.worker.written").description("chunks acknowledged by the shard writer").register(r);
        retries = Counter.builder("vs.worker.write.retries").description("retriable shard write failures").register(r);
        dlq = Counter.builder("vs.worker.dlq").description("records sent to raw-chunks.dlq").register(r);
        commits = Counter.builder("vs.worker.commits").register(r);
        embed = Timer.builder("vs.worker.embed").publishPercentileHistogram().register(r);
        write = Timer.builder("vs.worker.write").publishPercentileHistogram().register(r);
        batchSize = DistributionSummary.builder("vs.worker.batch.size").register(r);
    }
}
