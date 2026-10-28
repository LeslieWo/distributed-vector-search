package dev.vectorsearch.shardnode.index;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.nio.charset.StandardCharsets;

/**
 * Snapshot-only form of a {@link ChunkItem} (BUILDLOG step 37): id and text as length-prefixed UTF-8, the vector
 * as fp32 or IEEE half precision (fp16). Externalizable, so each item costs its bytes plus a few bytes of stream
 * framing instead of a full Java-serialized record. Reading turns it back into a ChunkItem with an fp32 vector
 * (for fp16: the nearest half-precision value of each component, relative error <= 2^-11).
 */
public final class DiskChunkItem implements Externalizable {
    private static final long serialVersionUID = 1L;

    private ChunkItem item;
    private boolean fp16;

    public DiskChunkItem() {} // Externalizable

    DiskChunkItem(ChunkItem item, boolean fp16) {
        this.item = item;
        this.fp16 = fp16;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        writeString(out, item.id());
        float[] v = item.vector();
        out.writeBoolean(fp16);
        out.writeShort(v.length);
        if (fp16) for (float x : v) out.writeShort(Float.floatToFloat16(x));
        else for (float x : v) out.writeFloat(x);
        writeString(out, item.text() == null ? "" : item.text());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        String id = readString(in);
        fp16 = in.readBoolean();
        float[] v = new float[in.readShort()];
        if (fp16) for (int i = 0; i < v.length; i++) v[i] = Float.float16ToFloat(in.readShort());
        else for (int i = 0; i < v.length; i++) v[i] = in.readFloat();
        item = new ChunkItem(id, v, readString(in));
    }

    private Object readResolve() throws ObjectStreamException {
        return item;
    }

    private static void writeString(ObjectOutput out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(ObjectInput in) throws IOException {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
