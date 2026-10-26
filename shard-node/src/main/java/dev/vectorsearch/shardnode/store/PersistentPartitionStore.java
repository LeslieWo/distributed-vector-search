package dev.vectorsearch.shardnode.store;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.vectorsearch.proto.v1.ChunkVector;
import dev.vectorsearch.proto.v1.DeleteRequest;
import dev.vectorsearch.proto.v1.UpsertRequest;
import dev.vectorsearch.shardnode.ShardNodeProperties;
import dev.vectorsearch.shardnode.index.HnswVectorIndex;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition store that survives restarts: periodic per-partition HNSW snapshots + a write-ahead log
 * of everything applied since the last snapshot.
 *
 * <p>Startup: load every {@code <partition>.hnsw}, then replay {@code wal.log}. Replaying a write that
 * is already in the snapshot is harmless because upserts are idempotent by chunk id, so the snapshot
 * and the WAL truncation do not need to be atomic with each other.
 */
public class PersistentPartitionStore extends PartitionStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PersistentPartitionStore.class);
    private static final String SNAPSHOT_SUFFIX = ".hnsw";

    private final Path dir;
    private final WriteAheadLog wal;
    // Writes hold the read side (many concurrent writers); a snapshot holds the write side so the
    // files it writes and the WAL truncation see a frozen state. Searches never take this lock.
    private final ReentrantReadWriteLock writeGate = new ReentrantReadWriteLock();
    private final AtomicLong writesSinceSnapshot = new AtomicLong();
    private final ScheduledExecutorService scheduler;
    private volatile SnapshotListener listener = (millis, partitions) -> {};

    public interface SnapshotListener {
        void onSnapshot(long millis, int partitions);
    }

    public PersistentPartitionStore(ShardNodeProperties props) {
        super(props);
        this.dir = props.dataDir();
        try {
            Files.createDirectories(dir);
            restore();
            this.wal = new WriteAheadLog(dir.resolve("wal.log"));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot restore shard state from " + dir, e);
        }
        long every = props.snapshotInterval().toMillis();
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("snapshotter").daemon().factory());
        scheduler.scheduleWithFixedDelay(this::snapshotQuietly, every, every, TimeUnit.MILLISECONDS);
    }

    public void setSnapshotListener(SnapshotListener listener) {
        this.listener = listener;
    }

    private void restore() throws IOException {
        long t0 = System.nanoTime();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + SNAPSHOT_SUFFIX)) {
            for (Path f : files) {
                String partition = f.getFileName().toString().replace(SNAPSHOT_SUFFIX, "");
                try (InputStream in = new BufferedInputStream(Files.newInputStream(f), 1 << 20)) {
                    partitions.put(partition, HnswVectorIndex.load(in, properties().hnswEfSearch()));
                }
            }
        }
        int replayed = WriteAheadLog.replay(dir.resolve("wal.log"), this::applyLogged);
        writesSinceSnapshot.set(replayed); // replayed writes are not in a snapshot yet
        log.info("restored {} from {} in {} ms (+{} WAL records)", sizes(), dir, (System.nanoTime() - t0) / 1_000_000, replayed);
    }

    private void applyLogged(byte type, byte[] payload) {
        try {
            if (type == WriteAheadLog.UPSERT) {
                // Same parallel batch path as a live Upsert (records stay in order, a batch is parallel).
                List<Entry> entries = new ArrayList<>();
                for (ChunkVector c : UpsertRequest.parseFrom(payload).getChunksList()) {
                    float[] v = new float[c.getVectorCount()];
                    for (int i = 0; i < v.length; i++) v[i] = c.getVector(i);
                    entries.add(new Entry(c.getChunkId(), c.getPartition(), v, c.getText()));
                }
                super.upsert(entries);
            } else if (type == WriteAheadLog.DELETE) {
                DeleteRequest d = DeleteRequest.parseFrom(payload);
                super.delete(d.getPartition(), d.getChunkIdsList());
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("corrupt WAL payload with valid checksum", e);
        }
    }

    @Override
    public int upsert(List<Entry> entries) {
        UpsertRequest.Builder rec = UpsertRequest.newBuilder();
        for (Entry e : entries) {
            ChunkVector.Builder c = ChunkVector.newBuilder().setChunkId(e.chunkId()).setPartition(e.partition()).setText(e.text() == null ? "" : e.text());
            for (float x : e.vector()) c.addVector(x);
            rec.addChunks(c);
        }
        writeGate.readLock().lock();
        try {
            wal.append(WriteAheadLog.UPSERT, rec.build().toByteArray()); // durable before applied/acked
            writesSinceSnapshot.incrementAndGet();
            return super.upsert(entries);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL append failed", e);
        } finally {
            writeGate.readLock().unlock();
        }
    }

    @Override
    public int delete(String partition, List<String> chunkIds) {
        writeGate.readLock().lock();
        try {
            wal.append(WriteAheadLog.DELETE, DeleteRequest.newBuilder().setPartition(partition).addAllChunkIds(chunkIds).build().toByteArray());
            writesSinceSnapshot.incrementAndGet();
            return super.delete(partition, chunkIds);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL append failed", e);
        } finally {
            writeGate.readLock().unlock();
        }
    }

    /** Writes every partition to {@code <partition>.hnsw} (tmp file + fsync + atomic rename), then empties the WAL. */
    public void snapshot() throws IOException {
        writeGate.writeLock().lock();
        try {
            if (writesSinceSnapshot.get() == 0) return;
            long t0 = System.nanoTime();
            for (Map.Entry<String, HnswVectorIndex> e : partitions.entrySet()) {
                Path target = dir.resolve(e.getKey() + SNAPSHOT_SUFFIX);
                Path tmp = dir.resolve(e.getKey() + SNAPSHOT_SUFFIX + ".tmp");
                try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tmp), 1 << 20)) {
                    e.getValue().save(out);
                }
                try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                // rename is atomic: a crash leaves either the old or the new snapshot, never half of one
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            wal.truncate();
            writesSinceSnapshot.set(0);
            long millis = (System.nanoTime() - t0) / 1_000_000;
            log.info("snapshot of {} written in {} ms", sizes(), millis);
            listener.onSnapshot(millis, partitions.size());
        } finally {
            writeGate.writeLock().unlock();
        }
    }

    private void snapshotQuietly() {
        try {
            snapshot();
        } catch (Exception e) {
            log.error("snapshot failed; WAL keeps the data, will retry next interval", e);
        }
    }

    public long walSizeBytes() {
        try {
            return wal.sizeBytes();
        } catch (IOException e) {
            return -1;
        }
    }

    /** Final snapshot on graceful shutdown, so the next start does not need to replay the WAL. */
    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        snapshot();
        wal.close();
    }
}
