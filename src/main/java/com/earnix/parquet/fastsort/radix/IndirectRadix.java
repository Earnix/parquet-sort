package com.earnix.parquet.fastsort.radix;

import java.util.Arrays;
import java.util.function.IntToLongFunction;

/**
 * Indirect radix entrypoint.
 *
 * Callers provide an index->sortable-long key extractor and a bytesPerDigit
 * parameter.
 * The implementation precomputes keys, then performs an indirect LSD radix
 * using the chosen digit width.
 */
public final class IndirectRadix {
    private IndirectRadix() {
    }

    private static int[] sortIndexed(int length, IntToLongFunction keyExtractor, int bytesPerDigit) {
        if (length == 0)
            return new int[0];
        long[] keys = new long[length];
        for (int i = 0; i < length; i++)
            keys[i] = keyExtractor.applyAsLong(i);

        int[] perm = new int[length];
        for (int i = 0; i < length; i++)
            perm[i] = i;
        int[] tmp = new int[length];

        final int passes = Long.BYTES / bytesPerDigit;
        final int radixBits = 8 * bytesPerDigit;
        final int buckets = 1 << radixBits;
        int[] count = new int[buckets];

        long mask = (1L << radixBits) - 1L;
        for (int pass = 0; pass < passes; pass++) {
            int shift = pass * radixBits;
            Arrays.fill(count, 0);
            for (int i = 0; i < length; i++) {
                long key = keys[perm[i]];
                int b = (int) ((key >>> shift) & mask);
                count[b]++;
            }
            int sum = 0;
            for (int i = 0; i < buckets; i++) {
                int c = count[i];
                count[i] = sum;
                sum += c;
            }
            for (int i = 0; i < length; i++) {
                int p = perm[i];
                long key = keys[p];
                int b = (int) ((key >>> shift) & mask);
                tmp[count[b]++] = p;
            }
            int[] swap = perm;
            perm = tmp;
            tmp = swap;
        }
        return perm;
    }

    /**
     * Fast-path for 32-bit ints: flip sign bit, then radix-sort.
     */
    public static int[] sortInts(int[] values) {
        final int n = values.length;
        final int SIGN_FLIP = 0x80000000;
        return sortIndexed(n, i -> (long) (values[i] ^ SIGN_FLIP) & 0xffffffffL, 1);
    }

    /**
     * Fast-path for 64-bit longs: flip sign bit, then radix-sort.
     */
    public static int[] sortLongs(long[] values) {
        final int n = values.length;
        final long SIGN_FLIP = 0x8000000000000000L;
        return sortIndexed(n, i -> values[i] ^ SIGN_FLIP, 1);
    }

    /**
     * Fast-path for doubles: reinterpret bits and map to sortable long, then
     * radix-sort.
     */
    public static int[] sortDoubles(double[] values) {
        final int n = values.length;
        return sortIndexed(n, i -> {
            long bits = Double.doubleToRawLongBits(values[i]);
            // Convert IEEE-754 ordering to lexicographic long ordering
            return bits ^ ((bits >> 63) & 0x7fffffffffffffffL);
        }, 1);
    }
}
