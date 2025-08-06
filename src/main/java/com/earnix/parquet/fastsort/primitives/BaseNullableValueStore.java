package com.earnix.parquet.fastsort.primitives;

/**
 * A base class for primitives arrays that may have null values
 */
public abstract class BaseNullableValueStore
{
	// whether the primitive is null. Or null if there are no null values
	protected final boolean[] isNull;
	private final int size; // size of the array

	protected BaseNullableValueStore(boolean[] isNull, int size)
	{
		this.isNull = isNull;
		this.size = size;
	}

	/**
	 * Whether the value at the specified index is null
	 *
	 * @param idx the idx to check if it is nul
	 * @return whether the idx is null
	 */
	public boolean isNull(int idx)
	{
		return isNull != null && isNull[idx];
	}

	/**
	 * @return the number of elements in this values array
	 */
	public int size()
	{
		return size;
	}

	/**
	 * @return whether this nullable value store contains any null values.
	 */
	public boolean hasNullValues()
	{
		if (isNull == null)
			return false;
		for (boolean isNull : isNull)
		{
			if (isNull)
			{
				return true;
			}
		}
		return false;
	}
}
