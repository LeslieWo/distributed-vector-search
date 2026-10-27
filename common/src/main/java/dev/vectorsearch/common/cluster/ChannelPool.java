package dev.vectorsearch.common.cluster;

import io.grpc.ManagedChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** One long-lived gRPC channel per shard address (channels are expensive; they multiplex many calls). */
public final class ChannelPool implements AutoCloseable {
    private final Function<String, ManagedChannel> factory;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    /** @param factory builds a channel for "host:port" (callers pick the transport, e.g. Netty or in-process) */
    public ChannelPool(Function<String, ManagedChannel> factory) {
        this.factory = factory;
    }

    public ManagedChannel get(String address) {
        return channels.computeIfAbsent(address, factory);
    }

    /**
     * Closes channels to addresses that are no longer live. Besides freeing resources this matters for recovery:
     * a channel that kept failing to connect sits in gRPC's exponential reconnect backoff (1 s, 1.6 s, 2.6 s, ...
     * up to 120 s) and fails calls instantly until the timer fires, even after the node is back. Dropping it when
     * the node's heartbeat disappears means the node gets a fresh channel, which connects at once, when it returns.
     */
    public void retainOnly(Set<String> addresses) {
        channels.entrySet().removeIf(e -> {
            if (addresses.contains(e.getKey())) return false;
            e.getValue().shutdownNow();
            return true;
        });
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
