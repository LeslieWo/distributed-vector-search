package dev.vectorsearch.common.cluster;

/** Supplies the latest {@link ClusterView}; implementations refresh it in the background. */
public interface ClusterViewSource extends AutoCloseable {
    ClusterView view();

    @Override
    default void close() {}
}
