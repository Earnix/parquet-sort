package com.earnix.parquet.fastsort;

import com.earnix.parquet.fastsort.primitives.DoubleVals;
import com.earnix.parquet.fastsort.primitives.IntVals;
import com.earnix.parquet.fastsort.primitives.LongVals;
import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;

public class ColumnReader
{
	public static final byte NULL_BYTE = (byte) 0;
	public static final byte FALSE_BYTE = (byte) 1;
	public static final byte TRUE_BYTE = (byte) 2;

	private final IndexedParquetColumnarFileReader reader;
	private final int[] srcIdx2DstIndexMap;

	public static ColumnReader createInOrderColumnReader(IndexedParquetColumnarFileReader reader)
	{
		return new ColumnReader(reader, null);
	}

	public static ColumnReader createReorderedColumnReader(IndexedParquetColumnarFileReader reader,
			int[] srcIdx2DstIndexMap)
	{
		return new ColumnReader(reader, requireNonNull(srcIdx2DstIndexMap, "map must not be null"));
	}

	private ColumnReader(IndexedParquetColumnarFileReader reader, int[] srcIdx2DstIndexMap)
	{
		this.reader = requireNonNull(reader, "reader must not be null");
		this.srcIdx2DstIndexMap = srcIdx2DstIndexMap;
	}

	public byte[][] readBinaryCol(ColumnDescriptor descriptor) throws IOException
	{
		if (descriptor.getPrimitiveType().getPrimitiveTypeName() != BINARY
				&& descriptor.getPrimitiveType().getPrimitiveTypeName() != FIXED_LEN_BYTE_ARRAY)
			throw new IllegalArgumentException("Unexpected type: " + descriptor + " expected: " + BINARY);

		return readCol(descriptor, new PrimitiveStoreBuilder<>()
		{
			byte[][] vals;

			@Override
			public void init(int size)
			{
				vals = new byte[size][];
			}

			@Override
			public void set(int idx, ChunkValuesReader reader)
			{
				Binary binary = reader.getBinary();
				vals[idx] = binary.isBackingBytesReused() ? binary.getBytes() : binary.getBytesUnsafe();
			}

			@Override
			public boolean needNullArray()
			{
				return false;
			}

			@Override
			public byte[][] makeImmutableStore(boolean[] nullIndexes)
			{
				return vals;
			}
		});
	}

	public byte[] readBooleanCol(ColumnDescriptor descriptor) throws IOException
	{
		assertExpectedColType(descriptor, PrimitiveType.PrimitiveTypeName.BOOLEAN);

		return readCol(descriptor, new PrimitiveStoreBuilder<>()
		{
			byte[] vals;

			@Override
			public void init(int size)
			{
				vals = new byte[size];
			}

			@Override
			public void set(int idx, ChunkValuesReader reader)
			{
				vals[idx] = reader.getBoolean() ? TRUE_BYTE : FALSE_BYTE;
			}

			@Override
			public boolean needNullArray()
			{
				return false;
			}

			@Override
			public byte[] makeImmutableStore(boolean[] nullIndexes)
			{
				return vals;
			}
		});
	}

	public DoubleVals readDoubleCol(ColumnDescriptor descriptor) throws IOException
	{
		assertExpectedColType(descriptor, PrimitiveType.PrimitiveTypeName.DOUBLE);

		return readCol(descriptor, new PrimitiveStoreBuilder<>()
		{
			double[] vals;

			@Override
			public void init(int size)
			{
				vals = new double[size];
			}

			@Override
			public void set(int idx, ChunkValuesReader reader)
			{
				vals[idx] = reader.getDouble();
			}

			@Override
			public DoubleVals makeImmutableStore(boolean[] nullIndexes)
			{
				return new DoubleVals(nullIndexes, vals);
			}
		});
	}

	private static void assertExpectedColType(ColumnDescriptor descriptor, PrimitiveType.PrimitiveTypeName expected)
	{
		if (descriptor.getPrimitiveType().getPrimitiveTypeName() != expected)
			throw new IllegalArgumentException("Unexpected type: " + descriptor + " expected: " + expected);
	}

	public LongVals readLongCol(ColumnDescriptor descriptor) throws IOException
	{
		assertExpectedColType(descriptor, PrimitiveType.PrimitiveTypeName.INT64);

		return readCol(descriptor, new PrimitiveStoreBuilder<>()
		{
			long[] vals;

			@Override
			public void init(int size)
			{
				vals = new long[size];
			}

			@Override
			public void set(int idx, ChunkValuesReader reader)
			{
				vals[idx] = reader.getLong();
			}

			@Override
			public LongVals makeImmutableStore(boolean[] nullIndexes)
			{
				return new LongVals(nullIndexes, vals);
			}
		});
	}

	public IntVals readIntCol(ColumnDescriptor descriptor) throws IOException
	{
		assertExpectedColType(descriptor, PrimitiveType.PrimitiveTypeName.INT32);

		return readCol(descriptor, new PrimitiveStoreBuilder<>()
		{
			int[] vals;

			@Override
			public void init(int size)
			{
				vals = new int[size];
			}

			@Override
			public void set(int idx, ChunkValuesReader reader)
			{
				vals[idx] = reader.getInteger();
			}

			@Override
			public IntVals makeImmutableStore(boolean[] nullIndexes)
			{
				return new IntVals(nullIndexes, vals);
			}
		});
	}

	public IntVals readFloatCol(ColumnDescriptor descriptor) throws IOException
	{
		assertExpectedColType(descriptor, PrimitiveType.PrimitiveTypeName.FLOAT);

		return readCol(descriptor, new PrimitiveStoreBuilder<>()
		{
			int[] vals;

			@Override
			public void init(int size)
			{
				vals = new int[size];
			}

			@Override
			public void set(int idx, ChunkValuesReader reader)
			{
				vals[idx] = Float.floatToRawIntBits(reader.getFloat());
			}

			@Override
			public IntVals makeImmutableStore(boolean[] nullIndexes)
			{
				return new IntVals(nullIndexes, vals);
			}
		});
	}

	private <T> T readCol(ColumnDescriptor descriptor, PrimitiveStoreBuilder<T> primitiveStore) throws IOException
	{
		int size = toIntExact(reader.getTotalNumRows());
		primitiveStore.init(size);
		return ColumnReaderUtil.readColIntoMemory(reader, descriptor, primitiveStore, srcIdx2DstIndexMap);
	}

	public interface PrimitiveStoreBuilder<T>
	{
		void init(int size);

		void set(int idx, ChunkValuesReader reader);

		T makeImmutableStore(boolean[] nullIndexes);

		default boolean needNullArray()
		{
			return true;
		}
	}
}
