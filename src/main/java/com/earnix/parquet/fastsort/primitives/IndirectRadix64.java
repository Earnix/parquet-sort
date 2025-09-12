package com.earnix.parquet.fastsort.primitives;

/**
 * Indirect radix for 64-bit keys (longs) and double bit representations.
 */
public final class IndirectRadix64 {
    private IndirectRadix64() {}

    public static int[] sortLongs(long[] values) {
        final int n = values.length;
        int[] perm = new int[n];
        for (int i = 0; i < n; i++) perm[i] = i;
        int[] tmp = new int[n];
        int[] count = new int[256];
        for (int pass = 0; pass < 8; pass++) {
            int shift = pass * 8;
            for (int i = 0; i < 256; i++) count[i] = 0;
            for (int i = 0; i < n; i++) {
                long key = values[perm[i]] ^ 0x8000000000000000L; // flip sign bit
                int b = (int)((key >>> shift) & 0xFFL);
                count[b]++;
            }
            int sum = 0;
            for (int i = 0; i < 256; i++) {
                int c = count[i];
                count[i] = sum;
                sum += c;
            }
            for (int i = 0; i < n; i++) {
                int p = perm[i];
                long key = values[p] ^ 0x8000000000000000L;
                int b = (int)((key >>> shift) & 0xFFL);
                tmp[count[b]++] = p;
            }
            int[] swap = perm; perm = tmp; tmp = swap;
        }
        return perm;
    }

    /**
     * Sorts doubles represented by their raw long bits array.
     * Each entry of vals is a Double.doubleToRawLongBits encoding.
     */
    public static int[] sortDoubleBits(long[] vals) {
        final int n = vals.length;
        long[] keys = new long[n];
        for (int i = 0; i < n; i++) {
            long bits = vals[i];
            if (bits < 0L) bits = ~bits;
            else bits ^= 0x8000000000000000L;
            keys[i] = bits;
        }
        int[] perm = new int[n];
        for (int i = 0; i < n; i++) perm[i] = i;
        int[] tmp = new int[n];
        int[] count = new int[256];
        for (int pass = 0; pass < 8; pass++) {
            int shift = pass * 8;
            for (int i = 0; i < 256; i++) count[i] = 0;
            for (int i = 0; i < n; i++) {
                int b = (int)((keys[perm[i]] >>> shift) & 0xFFL);
                count[b]++;
            }
            int sum = 0;
            for (int i = 0; i < 256; i++) {
                int c = count[i];
                count[i] = sum;
                sum += c;
            }
            for (int i = 0; i < n; i++) {
                int p = perm[i];
                int b = (int)((keys[p] >>> shift) & 0xFFL);
                tmp[count[b]++] = p;
            }
            int[] swap = perm; perm = tmp; tmp = swap;
        }
        return perm;
    }
}
