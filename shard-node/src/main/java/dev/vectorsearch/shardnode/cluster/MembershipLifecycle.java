package dev.vectorsearch.shardnode.cluster;

import dev.vectorsearch.common.cluster.Heartbeater;
import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.grpc.GrpcServerLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Registers this node in Redis ({@code vs:nodes:<nodeId>} = host:grpcPort, TTL 3 s, refreshed every 1 s).
 *
 * <p>Ordering matters, which is why this runs in a later lifecycle phase than the gRPC server:
 * start = after the port is listening (readers never get an address that refuses connections);
 * stop = before the gRPC server: delete the key, give readers a moment to drop us, then the server drains.
 */
public class MembershipLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(MembershipLifecycle.class);

    private final ShardNodeProperties props;
    private final GrpcServerLifecycle grpc;
    private volatile Heartbeater heartbeater;

    public MembershipLifecycle(ShardNodeProperties props, GrpcServerLifecycle grpc) {
        this.props = props;
        this.grpc = grpc;
    }

    @Override
    public void start() {
        if (props.redisUri().isBlank()) {
            log.info("shard.redis-uri is blank: not registering in cluster membership");
            return;
        }
        heartbeater = new Heartbeater(props.redisUri(), props.nodeId(), props.advertiseHost() + ":" + grpc.port(),
                props.heartbeatInterval(), props.heartbeatTtl());
    }

    @Override
    public void stop() {
        Heartbeater hb = heartbeater;
        if (hb == null) return;
        hb.close();
        heartbeater = null;
        try {
            Thread.sleep(props.deregisterGrace().toMillis()); // readers poll membership every 500 ms
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return heartbeater != null;
    }

    @Override
    public int getPhase() {
        return GrpcServerLifecycle.PHASE + 1;
    }
}
