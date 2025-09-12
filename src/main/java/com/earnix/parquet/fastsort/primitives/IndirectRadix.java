package com.earnix.parquet.fastsort.primitives;

/**
 * Indirect radix sort for 32-bit int keys.
 */
public final class IndirectRadix {
    private IndirectRadix() {}

    public static int[] sortInts(int[] values) {
        final int n = values.length;
        int[] perm = new int[n];
        for (int i = 0; i < n; i++) perm[i] = i;
        int[] tmp = new int[n];
        int[] count = new int[256];
        // flip sign to get unsigned lex order for signed ints
        final int SIGN_FLIP = 0x80000000;
        for (int pass = 0; pass < 4; pass++) {
            int shift = pass * 8;
            for (int i = 0; i < 256; i++) count[i] = 0;
            for (int i = 0; i < n; i++) {
                int key = values[perm[i]] ^ SIGN_FLIP;
                int b = (key >>> shift) & 0xFF;
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
                int key = values[p] ^ SIGN_FLIP;
                int b = (key >>> shift) & 0xFF;
                tmp[count[b]++] = p;
            }
            int[] swap = perm; perm = tmp; tmp = swap;
        }
        return perm;
    }
}
