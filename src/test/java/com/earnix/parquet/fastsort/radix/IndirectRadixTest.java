package com.earnix.parquet.fastsort.radix;

import org.junit.Test;

import static org.junit.Assert.*;

public class IndirectRadixTest {

    @Test
    public void testSortInts() {
        int[] vals = new int[] { 3, -1, 2, 0, -5 };
        int[] perm = IndirectRadix.sortInts(vals);
        int[] sorted = new int[vals.length];
        for (int i = 0; i < perm.length; i++)
            sorted[i] = vals[perm[i]];
        for (int i = 1; i < sorted.length; i++)
            assertTrue(sorted[i - 1] <= sorted[i]);
    }

    @Test
    public void testSortLongs() {
        long[] vals = new long[] { 3L, -1L, 2L, 0L, -5L };
        int[] perm = IndirectRadix.sortLongs(vals);
        long[] sorted = new long[vals.length];
        for (int i = 0; i < perm.length; i++)
            sorted[i] = vals[perm[i]];
        for (int i = 1; i < sorted.length; i++)
            assertTrue(sorted[i - 1] <= sorted[i]);
    }

    @Test
    public void testSortDoubles() {
        double[] vals = new double[] { 3.0, Double.NaN, -0.0, 0.0, -2.5, Double.POSITIVE_INFINITY };
        int[] perm = IndirectRadix.sortDoubles(vals);
        double[] sorted = new double[vals.length];
        for (int i = 0; i < perm.length; i++)
            sorted[i] = vals[perm[i]];
        int lastFiniteIdx = -1;
        for (int i = 0; i < sorted.length; i++) {
            if (Double.isFinite(sorted[i]))
                lastFiniteIdx = i;
            else
                break;
        }
        for (int i = 1; i <= lastFiniteIdx; i++)
            assertTrue(Double.compare(sorted[i - 1], sorted[i]) <= 0);
    }
}
