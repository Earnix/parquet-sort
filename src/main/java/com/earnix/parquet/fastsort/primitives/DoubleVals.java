package com.earnix.parquet.fastsort.primitives;

import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import shaded.parquet.it.unimi.dsi.fastutil.doubles.DoubleArrays;

import java.util.stream.IntStream;

public class DoubleVals extends BaseNullableValueStore
{
	private final double[] vals;

	public DoubleVals(boolean[] nullIdx, double[] vals)
	{
		super(nullIdx, vals.length);
		this.vals = vals;
	}

	public double getVal(int idx)
	{
		return vals[idx];
	}

	public NullableIterators.NullableDoubleIterator iterator(int startInclusive, int endExclusive)
	{
		return new DoubleVals.Iterator(startInclusive, endExclusive);
	}

	public class Iterator extends RangedNullableBaseIterator implements NullableIterators.NullableDoubleIterator
	{
		public Iterator(int startInclusive, int endExclusive)
		{
			super(DoubleVals.this::isNull, startInclusive, endExclusive);
		}

		@Override
		public double getValue()
		{
			assertNotNull();
			return getVal(getCurrIdx());
		}
	}

	/**
	 * @return compute a map of source to destination indices assuming a sort by this column
	 */
	public int[] src2DestIndexMapByDouble()
	{
		// if null values exist, radix sort cannot be used because the int array doesn't know about nulls.
		if (hasNullValues())
		{
			return nullableSort();
		}
		else
		{
			int[] perm = IntStream.range(0, vals.length).toArray();
			DoubleArrays.parallelRadixSortIndirect(perm, vals, true);
			return SortUtils.reverseIndices(perm);
		}
	}

	private int[] nullableSort()
	{
		return SortUtils.computeOrdering(vals.length, vals, isNull,
				(vals, idx1, idx2) -> Double.compare(vals[idx1], vals[idx2]));
	}
}
