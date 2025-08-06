package com.earnix.parquet.fastsort;

import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.function.Uncheck;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;

import java.io.IOException;
import java.util.stream.IntStream;

import static com.earnix.parquet.fastsort.ColumnReader.createInOrderColumnReader;
import static com.earnix.parquet.fastsort.primitives.SortUtils.computeOrdering;
import static java.lang.Math.toIntExact;

public class SortOrderer
{
	// Bucket sort produces a sort order faster than radix sort when it can be used. However, it's turned off
	// for this benchmark demo as writing a sort specifically for the benchmark dataset could be considered cheating.
	// It only improves the overall runtime by about 5% in this scenario, as most of the time is spent reading,
	// reordering, and writing columns.
	private static final boolean BUCKET_SORT_ENABLED = false;
	private static final int NO_BUCKET_SORT = -1;
	private final IndexedParquetColumnarFileReader reader;

	public SortOrderer(IndexedParquetColumnarFileReader reader)
	{
		this.reader = reader;
	}

	/**
	 * Compute an int array such that for an element at row i in the source data set and row j in the output data set,
	 * {@code array[i] = j}
	 *
	 * @param colName the name of the column to sort by
	 * @return the sort order
	 */
	public int[] computeSortOrder(String colName) throws IOException
	{
		var descriptor = this.reader.getDescriptorByPath(colName);

		Stopwatch stopwatch = Stopwatch.createStarted();
		BucketSortCheck bucketSortCheck = checkIfBucketSortPossible(descriptor, stopwatch);

		stopwatch.reset().start();

		int[] order;
		if (bucketSortCheck.range() > 0 || bucketSortCheck.range() > reader.getTotalNumRows())
			order = computeOrderingBucketSort(bucketSortCheck.range(), descriptor, bucketSortCheck.min());
		else
		{
			final ColumnReader colReader = createInOrderColumnReader(reader);
			order = switch (descriptor.getPrimitiveType().getPrimitiveTypeName())
			{
				case INT32 -> colReader.readIntCol(descriptor).src2DestIndexMapByInteger();
				case BOOLEAN -> computeOrdering(colReader.readBooleanCol(descriptor));
				case BINARY -> computeBinColOrdering(colReader, descriptor);
				case FIXED_LEN_BYTE_ARRAY -> computeBinColOrdering(colReader, descriptor);
				case FLOAT -> colReader.readFloatCol(descriptor).src2DestIndexMapByFloat();
				case INT64 -> colReader.readLongCol(descriptor).src2DestIndexMapByLong();
				case DOUBLE -> colReader.readDoubleCol(descriptor).src2DestIndexMapByDouble();
				case INT96 -> null;
			};

			if (order == null)
				throw new UnsupportedOperationException("col " + descriptor + " unsupported sort type.");
		}

		System.out.println("Sorting by " + colName + " took:" + stopwatch.elapsed());
		System.out.println("Number of rows: " + order.length);
		return order;
	}

	private BucketSortCheck checkIfBucketSortPossible(ColumnDescriptor descriptor, Stopwatch stopwatch)
			throws IOException
	{
		int range = NO_BUCKET_SORT;
		int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		if (BUCKET_SORT_ENABLED)
		{
			BUCKET_SORT_NOT_POSSIBLE:
			switch (descriptor.getPrimitiveType().getPrimitiveTypeName())
			{
				case INT32:
				{
					var metadata = reader.readMetaData();
					for (int i = 0; i < metadata.getRow_groupsSize(); i++)
					{
						var col = reader.getColumnChunk(i, descriptor);
						if (col.getMeta_data().isSetStatistics())
						{
							var stats = col.getMeta_data().getStatistics();
							min = Math.min(min, getInt(stats.getMin_value()));
							max = Math.max(max, getInt(stats.getMax_value()));
						}
						else
							break BUCKET_SORT_NOT_POSSIBLE;
					}
					range = max - min + 1;
					System.out.println(stopwatch.elapsed() + " min: " + min + " max: " + max + " range: " + range);
					break;
				}
				default:
					range = NO_BUCKET_SORT;
			}
		}
		BucketSortCheck bucketSortCheck = new BucketSortCheck(range, min);
		return bucketSortCheck;
	}

	private record BucketSortCheck(int range, int min)
	{
	}

	private static int[] computeBinColOrdering(ColumnReader colReader, ColumnDescriptor descriptor) throws IOException
	{
		return computeOrdering(colReader.readBinaryCol(descriptor));
	}

	private int[] computeOrderingBucketSort(int range, ColumnDescriptor descriptor, int min) throws IOException
	{
		// add one to each out to count the number of "null" occurrences.
		int[][] counts = new int[reader.getNumRowGroups()][range + 1];

		IntStream.range(0, reader.getNumRowGroups()).parallel().forEach(rgIdx -> {
			var inMemCol = Uncheck.get(() -> this.reader.readInMem(rgIdx, descriptor));
			ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemCol);
			do
			{
				if (chunkValuesReader.isNull())
				{
					counts[rgIdx][counts.length - 1]++;
				}
				else
				{
					counts[rgIdx][chunkValuesReader.getInteger() - min]++;
				}
			}
			while (chunkValuesReader.next());
		});

		int[] count = computeTotalCounts(counts);

		// offset matrix is the starting index of each value for each row group to allow parallel computation
		// of order matrix.
		int[][] offsetMatrix = computeOffsetMatrix(counts, count);

		int[] srcRow2DstRowMap = new int[toIntExact(reader.getTotalNumRows())];

		IntStream.range(0, reader.getNumRowGroups()).parallel().forEach(rgIdx -> {
			var inMemCol = Uncheck.get(() -> this.reader.readInMem(rgIdx, descriptor));
			ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemCol);

			long offset = 0;
			long startRow = Uncheck.getAsLong(() -> reader.startRow(rgIdx));
			do
			{
				int idxPlace;
				if (chunkValuesReader.isNull())
				{
					idxPlace = counts.length - 1;
				}
				else
				{
					idxPlace = chunkValuesReader.getInteger() - min;
				}
				int srcRow = (int) (startRow + offset);
				int dstRow = offsetMatrix[rgIdx][idxPlace]++;
				srcRow2DstRowMap[srcRow] = dstRow;
				offset++;
			}
			while (chunkValuesReader.next());
		});

		return srcRow2DstRowMap;
	}

	private int[] computeTotalCounts(int[][] counts) throws IOException
	{
		int[] count = new int[counts[0].length];
		for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++)
		{
			for (int i = 0; i < counts[0].length; i++)
			{
				count[i] += counts[rgIdx][i];
			}
		}
		return count;
	}

	private int[][] computeOffsetMatrix(int[][] counts, int[] count) throws IOException
	{
		int[][] offsetMatrix = new int[counts.length][counts[0].length];
		for (int i = 1; i < counts[0].length; i++)
		{
			offsetMatrix[0][i] = offsetMatrix[0][i - 1] + count[i - 1];
		}

		for (int rgIdx = 1; rgIdx < reader.getNumRowGroups(); rgIdx++)
		{
			for (int i = 0; i < counts[0].length; i++)
			{
				offsetMatrix[rgIdx][i] = offsetMatrix[rgIdx - 1][i] + counts[rgIdx - 1][i];
			}
		}
		return offsetMatrix;
	}

	public int getInt(byte[] bytes)
	{
		return Uncheck.getAsInt(() -> BytesUtils.readIntLittleEndian(bytes, 0));
	}
}
