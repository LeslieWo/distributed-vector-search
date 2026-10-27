package dev.vectorsearch.common.ring;

import java.nio.charset.StandardCharsets;

/**
 * MurmurHash3 x64 128-bit (Austin Appleby's reference), returning the first 64 bits (h1).
 * Same value as Guava {@code murmur3_128().hashBytes(b).asLong()} and Python {@code mmh3.hash64(b)[0]}.
 *
 * <p>Why Murmur3 and not {@code String.hashCode()}: hashCode is 32-bit and poorly mixed (similar strings
 * like "doc#1", "doc#2" get nearby values), which clusters keys on the ring. Murmur3 is fast and every
 * input bit affects every output bit, so keys and vnodes spread uniformly.
 */
public final class Murmur3 {
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private Murmur3() {}

    public static long hash64(String s) {
        return hash64(s.getBytes(StandardCharsets.UTF_8));
    }

    public static long hash64(byte[] data) {
        int len = data.length;
        int nblocks = len / 16;
        long h1 = 0, h2 = 0; // seed 0

        for (int i = 0; i < nblocks; i++) {
            long k1 = getLong(data, i * 16);
            long k2 = getLong(data, i * 16 + 8);
            k1 *= C1; k1 = Long.rotateLeft(k1, 31); k1 *= C2; h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;
            k2 *= C2; k2 = Long.rotateLeft(k2, 33); k2 *= C1; h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
        }

        int tail = nblocks * 16;
        long k1 = 0, k2 = 0;
        switch (len & 15) {
            case 15: k2 ^= (long) (data[tail + 14] & 0xff) << 48;
            case 14: k2 ^= (long) (data[tail + 13] & 0xff) << 40;
            case 13: k2 ^= (long) (data[tail + 12] & 0xff) << 32;
            case 12: k2 ^= (long) (data[tail + 11] & 0xff) << 24;
            case 11: k2 ^= (long) (data[tail + 10] & 0xff) << 16;
            case 10: k2 ^= (long) (data[tail + 9] & 0xff) << 8;
            case 9:
                k2 ^= (data[tail + 8] & 0xff);
                k2 *= C2; k2 = Long.rotateLeft(k2, 33); k2 *= C1; h2 ^= k2;
            case 8: k1 ^= (long) (data[tail + 7] & 0xff) << 56;
            case 7: k1 ^= (long) (data[tail + 6] & 0xff) << 48;
            case 6: k1 ^= (long) (data[tail + 5] & 0xff) << 40;
            case 5: k1 ^= (long) (data[tail + 4] & 0xff) << 32;
            case 4: k1 ^= (long) (data[tail + 3] & 0xff) << 24;
            case 3: k1 ^= (long) (data[tail + 2] & 0xff) << 16;
            case 2: k1 ^= (long) (data[tail + 1] & 0xff) << 8;
            case 1:
                k1 ^= (data[tail] & 0xff);
                k1 *= C1; k1 = Long.rotateLeft(k1, 31); k1 *= C2; h1 ^= k1;
            default:
        }

        h1 ^= len; h2 ^= len;
        h1 += h2; h2 += h1;
        h1 = fmix(h1); h2 = fmix(h2);
        h1 += h2;
        return h1;
    }

    private static long getLong(byte[] b, int i) {
        return (b[i] & 0xffL) | (b[i + 1] & 0xffL) << 8 | (b[i + 2] & 0xffL) << 16 | (b[i + 3] & 0xffL) << 24
                | (b[i + 4] & 0xffL) << 32 | (b[i + 5] & 0xffL) << 40 | (b[i + 6] & 0xffL) << 48 | (b[i + 7] & 0xffL) << 56;
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
