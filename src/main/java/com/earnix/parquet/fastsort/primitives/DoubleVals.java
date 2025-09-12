package com.earnix.parquet.fastsort.primitives;

import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;

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
			// convert doubles to raw long bits and use IndirectRadix64.fast path
			long[] bits = new long[vals.length];
			for (int i = 0; i < vals.length; i++) bits[i] = Double.doubleToRawLongBits(vals[i]);
			return IndirectRadix64.sortDoubleBits(bits);
		}
	}

	private int[] nullableSort()
	{
		return SortUtils.computeOrdering(vals.length, vals, isNull,
				(vals, idx1, idx2) -> Double.compare(vals[idx1], vals[idx2]));
	}
}
