package dev.vectorsearch.shardnode.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs a grpc-java Netty server inside the Spring context: started after all beans are ready
 * (so restored indexes are in place before the first request), stopped gracefully on shutdown.
 */
public class GrpcServerLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

    /** Started before / stopped after anything that depends on the port being open (membership). */
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 100;

    private final int port;
    private final int threads;
    private final List<BindableService> services;
    private final List<ServerInterceptor> interceptors;
    private volatile Server server;
    private ExecutorService executor;

    public GrpcServerLifecycle(int port, int threads, List<BindableService> services) {
        this(port, threads, services, List.of());
    }

    public GrpcServerLifecycle(int port, int threads, List<BindableService> services, List<ServerInterceptor> interceptors) {
        this.port = port;
        this.threads = threads;
        this.services = services;
        this.interceptors = interceptors;
    }

    @Override
    public void start() {
        // Fixed pool: bounded concurrency instead of one thread per request under overload.
        executor = Executors.newFixedThreadPool(threads, Thread.ofPlatform().name("grpc-shard-", 0).factory());
        NettyServerBuilder b = NettyServerBuilder.forPort(port).executor(executor).maxInboundMessageSize(32 * 1024 * 1024);
        services.forEach(svc -> b.addService(ServerInterceptors.intercept(svc, interceptors)));
        try {
            server = b.build().start();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot bind gRPC port " + port, e);
        }
        log.info("gRPC server listening on port {}", server.getPort());
        // Netty's threads are daemons; without one non-daemon thread the JVM would exit right after
        // main() returns when nothing else (e.g. an HTTP server) keeps it alive.
        Server started = server;
        Thread.ofPlatform().name("grpc-await").daemon(false).start(() -> {
            try {
                started.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public void stop() {
        Server s = server;
        if (s == null) return;
        s.shutdown(); // stop accepting, let in-flight calls finish
        try {
            if (!s.awaitTermination(10, TimeUnit.SECONDS)) s.shutdownNow();
        } catch (InterruptedException e) {
            s.shutdownNow();
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
        server = null;
        log.info("gRPC server stopped");
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /** Actual bound port (useful when configured with 0 in tests). */
    public int port() {
        return server.getPort();
    }
}
