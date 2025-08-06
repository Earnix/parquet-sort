package com.earnix.parquet.fastsort.primitives;

import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;

import java.util.function.IntPredicate;

import static java.lang.Math.incrementExact;

/**
 * Base iterator class for iterating through a nullable column.
 */
public abstract class RangedNullableBaseIterator implements NullableIterators.NullableIterator
{
	private int currIdx;
	private final IntPredicate isNull;
	private final int startInclusive;
	private final int endExclusive;

	public RangedNullableBaseIterator(IntPredicate isNull, int startInclusive, int endExclusive)
	{
		this.isNull = isNull;
		this.startInclusive = startInclusive;
		this.endExclusive = endExclusive;
		currIdx = startInclusive - 1;
	}

	void sanityCheckInRange()
	{
		if (currIdx < startInclusive || currIdx >= endExclusive)
			throw new IllegalStateException();
	}

	void assertNotNull()
	{
		if (isNull())
			throw new NullPointerException();
	}

	@Override
	public boolean isNull()
	{
		sanityCheckInRange();
		return isNull.test(getCurrIdx());
	}

	@Override
	public boolean next()
	{
		incrementIdx();
		return currIdx < endExclusive;
	}

	int getCurrIdx()
	{
		return currIdx;
	}

	void incrementIdx()
	{
		currIdx = incrementExact(currIdx);
	}
}
