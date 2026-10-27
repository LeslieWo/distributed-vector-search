package dev.vectorsearch.coordinator.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/** The coordinator's public gRPC server (SearchService), started after all beans are ready. */
public class CoordinatorGrpcServer implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(CoordinatorGrpcServer.class);

    private final int port;
    private final int threads;
    private final BindableService service;
    private volatile Server server;
    private ExecutorService executor;

    public CoordinatorGrpcServer(int port, int threads, BindableService service) {
        this.port = port;
        this.threads = threads;
        this.service = service;
    }

    @Override
    public void start() {
        // Fixed pool: a query thread blocks while its partitions are searched; bounded so overload queues.
        executor = Executors.newFixedThreadPool(threads, Thread.ofPlatform().name("grpc-coord-", 0).factory());
        try {
            server = NettyServerBuilder.forPort(port).executor(executor).addService(service).build().start();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot bind gRPC port " + port, e);
        }
        log.info("coordinator gRPC server listening on port {}", server.getPort());
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
        s.shutdown();
        try {
            if (!s.awaitTermination(10, TimeUnit.SECONDS)) s.shutdownNow();
        } catch (InterruptedException e) {
            s.shutdownNow();
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
        server = null;
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    public int port() {
        return server.getPort();
    }
}
