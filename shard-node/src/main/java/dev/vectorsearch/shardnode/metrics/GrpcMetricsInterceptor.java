package dev.vectorsearch.shardnode.metrics;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;

/**
 * Times every RPC from arrival to completion, tagged by method and gRPC status code.
 * One timer gives both latency and QPS in Prometheus:
 * <pre>
 *   QPS: sum by (method) (rate(vs_shard_rpc_seconds_count[1m]))
 *   p99: histogram_quantile(0.99, sum by (le, method) (rate(vs_shard_rpc_seconds_bucket[1m])))
 * </pre>
 * Buckets instead of client-side percentiles: buckets can be summed across nodes, percentiles cannot.
 */
public final class GrpcMetricsInterceptor implements ServerInterceptor {
    public static final String METRIC = "vs.shard.rpc";

    private final MeterRegistry registry;

    public GrpcMetricsInterceptor(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
        String method = call.getMethodDescriptor().getBareMethodName();
        Timer.Sample sample = Timer.start(registry);
        ServerCall<Q, R> timed = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                sample.stop(Timer.builder(METRIC)
                        .description("shard-node gRPC latency")
                        .tag("method", method)
                        .tag("status", status.getCode().name())
                        .publishPercentileHistogram()
                        .minimumExpectedValue(Duration.ofNanos(100_000))
                        .maximumExpectedValue(Duration.ofSeconds(5))
                        .register(registry));
                super.close(status, trailers);
            }
        };
        return next.startCall(timed, headers);
    }
}
