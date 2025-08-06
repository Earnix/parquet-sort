package com.earnix.parquet.fastsort;

import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.fastsort.primitives.DoubleVals;
import com.earnix.parquet.fastsort.primitives.IntVals;
import com.earnix.parquet.fastsort.primitives.LongVals;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.file.PathUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Main
{
	public static void main(String[] args) throws IOException
	{
		if (args.length > 3)
			usageAndExit("Too many arguments.");
		if (args.length < 2)
			usageAndExit("Not enough arguments.");

		Stopwatch entireProgram = Stopwatch.createStarted();
		Path srcFile = Path.of(args[0]);

		if (!Files.exists(srcFile))
			usageAndExit("Source file " + srcFile + " does not exist");
		if (!Files.isReadable(srcFile))
			usageAndExit("Source file " + srcFile + " is not readable");
		Path outSortFile = Path.of(args[1]).toAbsolutePath();
		if (Files.exists(outSortFile))
			usageAndExit("Out sort file " + outSortFile + " already exists. Aborting.");

		String sortCol = args[2];

		runSort(srcFile, sortCol, outSortFile);
		System.out.println("total runtime: " + entireProgram.elapsed());
	}

	private static void usageAndExit(String err)
	{
		System.err.println("Parquet Sort. A tool that reads, sources and writes parquet");
		System.err.println("Usage: <in parquet file> <out parquet file> <column name>");

		if (err != null)
			System.err.println("Error: " + err);

		System.exit(1);
	}

	public static void runSort(Path srcFile, String sortCol, Path outSortFile) throws IOException
	{
		IndexedParquetColumnarFileReader reader = new IndexedParquetColumnarFileReader(srcFile);
		if (reader.getColumnDescriptors().stream().noneMatch(cd -> sortCol.equals(cd.getPath()[0])))
			usageAndExit("sort column " + sortCol + " not found.");

		var reorderer = computeOrder(reader, sortCol);
		runReorder(outSortFile, reader, reorderer);
	}

	private static void runReorder(Path outFile, IndexedParquetColumnarFileReader reader, ColumnReader reorderer)
			throws IOException
	{
		MessageType messageType = reader.getMessageType();

		Path tmpDir = Files.createTempDirectory(outFile.getParent(), "parquet_sort_rg_tmp");
		Files.createDirectories(tmpDir);
		try
		{
			RowGrpWriters writers = new RowGrpWriters(tmpDir, messageType, reader.rowsPerRowGroup());
			writeTmpFilesWithRowGroups(reader, reorderer, writers);
			writers.finish(outFile);
		}
		finally
		{
			PathUtils.deleteDirectory(tmpDir);
		}
	}

	private static void writeTmpFilesWithRowGroups(IndexedParquetColumnarFileReader reader, ColumnReader reorderer,
			RowGrpWriters writers) throws IOException
	{
		for (ColumnDescriptor col : reader.getColumnDescriptors())
		{
			Stopwatch stopwatch = Stopwatch.createStarted();
			switch (col.getPrimitiveType().getPrimitiveTypeName())
			{
				case INT32:
				{
					IntVals sorted = reorderer.readIntCol(col);
					writers.writeIntCol(col, sorted);
					break;
				}
				case FLOAT:
				{
					IntVals sorted = reorderer.readFloatCol(col);
					writers.writeFloatCol(col, sorted);
					break;
				}
				case INT64:
				{
					LongVals sorted = reorderer.readLongCol(col);
					writers.writeLongCol(col, sorted);
					break;
				}
				case DOUBLE:
				{
					DoubleVals sorted = reorderer.readDoubleCol(col);
					writers.writeDoubleCol(col, sorted);
					break;
				}
				case BINARY:
				case FIXED_LEN_BYTE_ARRAY:
				{
					byte[][] sorted = reorderer.readBinaryCol(col);
					writers.writeBinaryCol(col, sorted);
					break;
				}
				case BOOLEAN:
				{
					byte[] sorted = reorderer.readBooleanCol(col);
					writers.writeBooleanCol(col, sorted);
					break;
				}
				default:
					throw new IllegalStateException(
							"not supported " + List.of(col.getPath()) + " " + col.getPrimitiveType()
									.getPrimitiveTypeName());
			}
			System.out.println("Read and reordered " + col + " in " + stopwatch.elapsed());
		}
	}

	private static ColumnReader computeOrder(IndexedParquetColumnarFileReader reader, String sortCol) throws IOException
	{
		int[] sortOrder = new SortOrderer(reader).computeSortOrder(sortCol);
		var reorderer = ColumnReader.createReorderedColumnReader(reader, sortOrder);
		return reorderer;
	}
}