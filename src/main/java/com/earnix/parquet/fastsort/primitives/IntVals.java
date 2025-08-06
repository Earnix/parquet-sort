package com.earnix.parquet.fastsort.primitives;

import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrays;

import java.util.stream.IntStream;

/**
 * A store for int or float values that might be null
 */
public class IntVals extends BaseNullableValueStore
{
	private final int[] vals;

	public IntVals(boolean[] nullIdx, int[] vals)
	{
		super(nullIdx, vals.length);
		this.vals = vals;
	}

	public int getVal(int idx)
	{
		return vals[idx];
	}

	public NullableIterators.NullableIntegerIterator iterator(int startInclusive, int endExclusive)
	{
		return new Iterator(startInclusive, endExclusive);
	}

	public class Iterator extends RangedNullableBaseIterator implements NullableIterators.NullableIntegerIterator
	{
		public Iterator(int startInclusive, int endExclusive)
		{
			super(IntVals.this::isNull, startInclusive, endExclusive);
		}

		@Override
		public int getValue()
		{
			assertNotNull();
			return getVal(getCurrIdx());
		}
	}

	public NullableIterators.NullableFloatIterator floatIterator(int startInclusive, int endExclusive)
	{
		return new FloatIterator(startInclusive, endExclusive);
	}

	public class FloatIterator extends RangedNullableBaseIterator implements NullableIterators.NullableFloatIterator
	{
		public FloatIterator(int startInclusive, int endExclusive)
		{
			super(IntVals.this::isNull, startInclusive, endExclusive);
		}

		@Override
		public float getValue()
		{
			assertNotNull();
			return Float.intBitsToFloat(getVal(getCurrIdx()));
		}
	}

	/**
	 * @return compute a map of source to destination indices assuming a sort by this column.
	 */
	public int[] src2DestIndexMapByInteger()
	{
		// if null values exist, radix sort cannot be used because the int array doesn't know about nulls.
		if (hasNullValues())
		{
			return nullableSort();
		}
		else
		{
			int[] perm = IntStream.range(0, vals.length).toArray();
			IntArrays.parallelRadixSortIndirect(perm, vals, true);
			return SortUtils.reverseIndices(perm);
		}
	}

	private int[] nullableSort()
	{
		return SortUtils.computeOrdering(vals.length, vals, isNull,
				(vals, idx1, idx2) -> Integer.compare(vals[idx1], vals[idx2]));
	}

	/**
	 * @return compute a map of source to destination indices assuming a sort by this column with a float comparison
	 */
	public int[] src2DestIndexMapByFloat()
	{
		return SortUtils.computeOrdering(vals.length, vals, isNull,
				(vals, idx1, idx2) -> Float.compare(Float.intBitsToFloat(vals[idx1]),
						Float.intBitsToFloat(vals[idx2])));
	}
}
