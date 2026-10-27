package dev.vectorsearch.common.ring;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class Murmur3Test {
    /** Reference values from Python mmh3 5.x: {@code mmh3.hash64(s.encode(), seed=0, signed=True)[0]}. */
    @Test
    void matchesReferenceImplementation() {
        assertEquals(0L, Murmur3.hash64(""));
        assertEquals(-3758069500696749310L, Murmur3.hash64("hello"));
        assertEquals(-5128882707986832260L, Murmur3.hash64("node-1#0"));
        assertEquals(2268690136896504895L, Murmur3.hash64("8841823#0"));
        assertEquals(-2068352364225029268L, Murmur3.hash64("The quick brown fox jumps over the lazy dog"));
        assertEquals(-4266531025627334877L, Murmur3.hash64("abcdefghijklmnop"));   // exactly one 16-byte block
        assertEquals(8459014091212432983L, Murmur3.hash64("abcdefghijklmnopq"));  // block + 1-byte tail
    }
}
