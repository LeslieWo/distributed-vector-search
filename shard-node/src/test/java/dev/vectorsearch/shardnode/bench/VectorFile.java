package dev.vectorsearch.shardnode.bench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** Dense matrix file for bench data: int32 rows, int32 cols, then rows*cols little-endian values. */
final class VectorFile {
    private VectorFile() {}

    static void writeFloats(Path file, float[][] rows) throws IOException {
        int cols = rows.length == 0 ? 0 : rows[0].length;
        Files.createDirectories(file.getParent());
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(8 + rows.length * cols * 4).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(rows.length).putInt(cols);
            for (float[] r : rows) for (float x : r) buf.putFloat(x);
            buf.flip();
            while (buf.hasRemaining()) ch.write(buf);
        }
    }

    static float[][] readFloats(Path file) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(file)).order(ByteOrder.LITTLE_ENDIAN);
        int rows = buf.getInt(), cols = buf.getInt();
        float[][] out = new float[rows][cols];
        for (float[] r : out) for (int c = 0; c < cols; c++) r[c] = buf.getFloat();
        return out;
    }

    static void writeInts(Path file, int[][] rows) throws IOException {
        int cols = rows.length == 0 ? 0 : rows[0].length;
        ByteBuffer buf = ByteBuffer.allocate(8 + rows.length * cols * 4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(rows.length).putInt(cols);
        for (int[] r : rows) for (int x : r) buf.putInt(x);
        Files.write(file, buf.array());
    }

    static int[][] readInts(Path file) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(file)).order(ByteOrder.LITTLE_ENDIAN);
        int rows = buf.getInt(), cols = buf.getInt();
        int[][] out = new int[rows][cols];
        for (int[] r : out) for (int c = 0; c < cols; c++) r[c] = buf.getInt();
        return out;
    }

    static List<String> readLines(Path file) throws IOException {
        return Files.readAllLines(file);
    }
}
