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

    /** Closes channels to addresses that are no longer in use. */
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
