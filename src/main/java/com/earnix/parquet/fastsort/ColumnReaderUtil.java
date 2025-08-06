package com.earnix.parquet.fastsort;

import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import org.apache.commons.io.function.Uncheck;
import org.apache.parquet.column.ColumnDescriptor;

import java.io.IOException;
import java.util.stream.IntStream;

import static java.lang.Math.toIntExact;

public class ColumnReaderUtil
{
	/**
	 * Read the specified column into memory in parallel
	 *
	 * @param reader             the reader for the parquet file
	 * @param descriptor         the column to read into memory
	 * @param primitiveStore     the store to build from the column
	 * @param srcIdx2DstIndexMap the map from source to destination index while reading the column
	 * @param <T>                the store type
	 * @return the build column store which contains the values
	 * @throws IOException on an IOException reading the file
	 */
	public static <T> T readColIntoMemory(IndexedParquetColumnarFileReader reader, ColumnDescriptor descriptor,
			ColumnReader.PrimitiveStoreBuilder<T> primitiveStore, int[] srcIdx2DstIndexMap) throws IOException
	{
		int size = toIntExact(reader.getTotalNumRows());
		primitiveStore.init(size);
		boolean[] nullIndexes = isNullable(descriptor) && primitiveStore.needNullArray() ? new boolean[size] : null;

		IntStream.range(0, reader.getNumRowGroups()).parallel().forEach(rgIdx -> {
			var inMemCol = Uncheck.get(() -> reader.readInMem(rgIdx, descriptor));
			ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemCol);
			int idx = toIntExact(Uncheck.getAsLong(() -> reader.startRow(rgIdx)));
			do
			{
				int dstIdx = srcIdx2DstIndexMap == null ? idx : srcIdx2DstIndexMap[idx];
				idx++;
				if (chunkValuesReader.isNull())
				{
					if (nullIndexes != null)
						nullIndexes[dstIdx] = true;
				}
				else
				{
					primitiveStore.set(dstIdx, chunkValuesReader);
				}
			}
			while (chunkValuesReader.next());
		});
		return primitiveStore.makeImmutableStore(nullIndexes);
	}

	private static boolean isNullable(ColumnDescriptor descriptor)
	{
		return descriptor.getMaxDefinitionLevel() > 0;
	}
}
