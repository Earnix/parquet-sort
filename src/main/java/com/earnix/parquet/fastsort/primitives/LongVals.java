package com.earnix.parquet.fastsort.primitives;

import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrays;
import shaded.parquet.it.unimi.dsi.fastutil.longs.LongArrays;

import java.util.stream.IntStream;

/**
 * A store for long or double values that might be null
 */
public class LongVals extends BaseNullableValueStore
{
	private final long[] vals;

	public LongVals(boolean[] nullIdx, long[] vals)
	{
		super(nullIdx, vals.length);
		this.vals = vals;
	}

	public long getVal(int idx)
	{
		return vals[idx];
	}

	public NullableIterators.NullableLongIterator iterator(int startInclusive, int endExclusive)
	{
		return new LongVals.Iterator(startInclusive, endExclusive);
	}

	public class Iterator extends RangedNullableBaseIterator implements NullableIterators.NullableLongIterator
	{
		public Iterator(int startInclusive, int endExclusive)
		{
			super(LongVals.this::isNull, startInclusive, endExclusive);
		}

		@Override
		public long getValue()
		{
			assertNotNull();
			return getVal(getCurrIdx());
		}
	}

	/**
	 * @return compute a map of source to destination indices assuming a sort by this column
	 */
	public int[] src2DestIndexMapByLong()
	{
		// if null values exist, radix sort cannot be used because the int array doesn't know about nulls.
		if (hasNullValues())
		{
			return nullableSort();
		}
		else
		{
			int[] perm = IntStream.range(0, vals.length).toArray();
			LongArrays.parallelRadixSortIndirect(perm, vals, true);
			return SortUtils.reverseIndices(perm);
		}
	}

	private int[] nullableSort()
	{
		return SortUtils.computeOrdering(vals.length, vals, isNull,
				(vals, idx1, idx2) -> Long.compare(vals[idx1], vals[idx2]));
	}
}
