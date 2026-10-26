package dev.vectorsearch.shardnode.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;

/**
 * Append-only log of writes applied since the last snapshot, so an acknowledged upsert survives
 * a crash (kill -9) even before the next snapshot.
 *
 * <p>Record: {@code [type:1][length:4][payload:length][crc32(type+payload):4]}. Every append is
 * fsynced before the write is acknowledged. On replay we stop at the first torn or corrupt record:
 * that write was never acknowledged, because the fsync did not complete.
 */
final class WriteAheadLog implements AutoCloseable {
    static final byte UPSERT = 1;
    static final byte DELETE = 2;

    private final Path file;
    private FileChannel channel;

    WriteAheadLog(Path file) throws IOException {
        this.file = file;
        this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    synchronized void append(byte type, byte[] payload) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(type);
        crc.update(payload);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + payload.length + 4);
        buf.put(type).putInt(payload.length).put(payload).putInt((int) crc.getValue()).flip();
        while (buf.hasRemaining()) channel.write(buf);
        channel.force(false);
    }

    /** Drops all records; call only after a snapshot that covers them has been made durable. */
    synchronized void truncate() throws IOException {
        channel.truncate(0);
        channel.force(true);
    }

    synchronized long sizeBytes() throws IOException {
        return channel.size();
    }

    /** @return number of records replayed */
    static int replay(Path file, BiConsumer<Byte, byte[]> apply) throws IOException {
        if (!Files.exists(file)) return 0;
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(file));
        int n = 0;
        while (buf.remaining() >= 5) {
            byte type = buf.get();
            int len = buf.getInt();
            if (len < 0 || buf.remaining() < len + 4) break; // torn tail
            byte[] payload = new byte[len];
            buf.get(payload);
            int stored = buf.getInt();
            CRC32 crc = new CRC32();
            crc.update(type);
            crc.update(payload);
            if ((int) crc.getValue() != stored) break; // corrupt tail
            apply.accept(type, payload);
            n++;
        }
        return n;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    Path file() {
        return file;
    }
}
