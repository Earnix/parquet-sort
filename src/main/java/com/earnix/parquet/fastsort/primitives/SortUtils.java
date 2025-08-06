package com.earnix.parquet.fastsort.primitives;

import com.earnix.parquet.fastsort.ColumnReader;
import com.google.common.math.IntMath;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrays;

import java.math.RoundingMode;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

/**
 * Sort util to compute a stable sort ordering via an indirect support, supporting null values
 */
public class SortUtils
{
	public static int[] computeOrdering(byte[] booleanVals)
	{
		return computeOrdering(booleanVals.length, booleanVals, i -> booleanVals[i] == ColumnReader.NULL_BYTE,
				(vals, a, b) -> Byte.compare(vals[a], vals[b]));
	}

	public static int[] computeOrdering(byte[][] binaryVals)
	{
		return computeOrdering(binaryVals.length, binaryVals, i -> binaryVals[i] == null,
				(vals, a, b) -> java.util.Arrays.compareUnsigned(vals[a], vals[b]));
	}

	public static <T> int[] computeOrdering(int len, T vals, boolean[] isNull,
			PrimitiveIndexComparator<T> primitiveComparator)
	{
		return computeOrdering(len, vals, i -> isNull != null && isNull[i], primitiveComparator);
	}

	public static <T> int[] computeOrdering(int len, T vals, IntPredicate isNull,
			PrimitiveIndexComparator<T> primitiveComparator)
	{
		int[] dest2srcIndex = IntStream.range(0, len).toArray();
		IntArrays.parallelQuickSort(dest2srcIndex, (a, b) -> {
			// if both are null, do a stable sort.
			if (isNull.test(a) && isNull.test(b))
				return Integer.compare(a, b);

			if (isNull.test(a) || isNull.test(b))
				return -Boolean.compare(isNull.test(a), isNull.test(b));

			int valComparison = primitiveComparator.compare(vals, a, b);

			if (valComparison == 0)
				return Integer.compare(a, b);
			return valComparison;
		});

		// reverse indexes;
		return reverseIndices(dest2srcIndex);
	}

	public static int[] reverseIndices(int[] idxs)
	{
		var src2dstRowIdx = new int[idxs.length];
		int blockSize = 1024 * 16;

		IntStream.range(0, IntMath.divide(idxs.length, blockSize, RoundingMode.CEILING)).parallel().forEach(start -> {
			int blockStart = start * blockSize;
			for (int i = blockStart; i < Math.min(blockStart + blockSize, idxs.length); i++)
			{
				src2dstRowIdx[idxs[i]] = i;
			}
		});
		return src2dstRowIdx;
	}

	@FunctionalInterface
	public interface PrimitiveIndexComparator<T>
	{
		int compare(T vals, int idx1, int idx2);
	}
}
