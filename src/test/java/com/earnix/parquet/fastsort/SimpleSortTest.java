package com.earnix.parquet.fastsort;

import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import org.apache.commons.io.file.PathUtils;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import shaded.parquet.it.unimi.dsi.fastutil.longs.Long2IntFunction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

@RunWith(Parameterized.class)
public class SimpleSortTest
{
	private final CompressionCodec compressionCodec;
	private final int numRowGroups;
	private final int numRowsPerRowGrp = 5;

	@Parameterized.Parameters(name = "{0} {1}")
	public static Collection<Object[]> data()
	{
		return List.of(//
				new Object[] { CompressionCodec.SNAPPY, 1 },//
				new Object[] { CompressionCodec.ZSTD, 2 },//
				new Object[] { CompressionCodec.UNCOMPRESSED, 3 });
	}

	public SimpleSortTest(CompressionCodec compressionCodec, int numRowGroups)
	{
		this.compressionCodec = compressionCodec;
		this.numRowGroups = numRowGroups;
	}

	/**
	 * Tests sorting a parquet file works and orders the rows correctly
	 */
	@Test
	public void simpleTest() throws IOException
	{
		Path outDir = Files.createTempDirectory("parquet_tmp");
		try
		{
			testInTmpDir(outDir);
		}
		finally
		{
			PathUtils.deleteDirectory(outDir);
		}
	}

	private void testInTmpDir(Path outDir) throws IOException
	{
		MessageType messageType = getMessageType();
		Path outFileUnsorted = outDir.resolve("test_" + compressionCodec + ".parquet");

		writeTestFile(outFileUnsorted, messageType, numRowsPerRowGrp);

		validateSort("int32", outFileUnsorted, false);
		validateSort("float", outFileUnsorted, false);
		validateSort("int64", outFileUnsorted, false);
		validateSort("int32_rev", outFileUnsorted, true);
		validateSort("double", outFileUnsorted, true);
		validateSort("binary", outFileUnsorted, false);
		validateSort("fixed_bin", outFileUnsorted, false);
	}

	private void validateSort(String sortColName, Path outFileUnsorted, boolean inRevOrder) throws IOException
	{
		// sort in order and in reverse order
		Path sorted = outFileUnsorted.resolveSibling("out_sorted.parquet");
		Main.runSort(outFileUnsorted, sortColName, sorted);

		IndexedParquetColumnarFileReader reader = new IndexedParquetColumnarFileReader(sorted);

		Assert.assertEquals(this.numRowGroups, reader.getNumRowGroups());

		for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++)
		{
			Long2IntFunction inOrder = row -> toIntExact(row / numRowGroups);
			assertExpectedVal(reader, rgIdx, inRevOrder ? "int32_rev" : "int32", inOrder);

			Long2IntFunction revOrder = row -> toIntExact(numRowsPerRowGrp - 1 - (row / numRowGroups));
			assertExpectedVal(reader, rgIdx, inRevOrder ? "int32" : "int32_rev", revOrder);

			assertExpectedVal(reader, rgIdx, "fixed_bin", (row, cvr) -> Assert.assertEquals(
					inRevOrder ? revOrder.apply(row).toString() : inOrder.apply(row).toString(),
					cvr.getBinary().toStringUsingUTF8()));
		}
		Files.delete(sorted);
	}

	private void assertExpectedVal(IndexedParquetColumnarFileReader reader, int rgIdx, String colName,
			Long2IntFunction expected) throws IOException
	{

		assertExpectedVal(reader, rgIdx, colName,
				(row, chunkValuesReader) -> Assert.assertEquals(expected.applyAsInt(row),
						chunkValuesReader.getInteger()));
	}

	private void assertExpectedVal(IndexedParquetColumnarFileReader reader, int rgIdx, String colName,
			BiConsumer<Long, ChunkValuesReader> expected) throws IOException
	{
		ChunkValuesReader chunkValuesReader = getChunkValuesReader(reader, rgIdx, colName);

		long row = reader.startRow(rgIdx);
		do
		{
			expected.accept(row, chunkValuesReader);
			row++;
		}
		while (chunkValuesReader.next());
	}

	private static ChunkValuesReader getChunkValuesReader(IndexedParquetColumnarFileReader reader, int rgIdx,
			String colName) throws IOException
	{
		var inMemCol = reader.readInMem(rgIdx, reader.getMessageType().getColumnDescription(new String[] { colName }));
		ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemCol);
		return chunkValuesReader;
	}

	private void writeTestFile(Path outFileUnsorted, MessageType messageType, int numRowsPerRowGrp) throws IOException
	{
		try (var writer = ParquetFileColumnarWriterFactory.createWriter(outFileUnsorted, messageType, compressionCodec,
				true))
		{
			for (int rgIdx = 0; rgIdx < numRowGroups; rgIdx++)
			{
				writer.writeRowGroup(numRowsPerRowGrp, rga -> {
					var it = messageType.getColumns().iterator();
					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
							IntStream.range(0, numRowsPerRowGrp).iterator()));
					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
							IntStream.iterate(4, i -> i - 1).limit(numRowsPerRowGrp).iterator()));

					rga.writeValues(
							chunkWriter -> chunkWriter.writeColumn(it.next(), LongStream.range(0, 5).iterator()));

					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
							IntStream.range(0, numRowsPerRowGrp).mapToObj(Integer::toString).toArray(String[]::new)));
					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
							IntStream.range(0, numRowsPerRowGrp).mapToObj(Integer::toString).toArray(String[]::new)));

					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
							IntStream.iterate(5, i -> i - 1).limit(numRowsPerRowGrp).mapToDouble(i -> i).iterator()));

					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(), orderedFloats(numRowsPerRowGrp)));

					Boolean[] bools = new Boolean[numRowsPerRowGrp];
					bools[0] = true;
					bools[bools.length - 1] = false;
					rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(), asList(bools).iterator()));
				});
			}
			writer.finishAndWriteFooterMetadata();
		}
	}

	private static float[] orderedFloats(int numRowsPerRowGrp)
	{
		float[] vals = new float[numRowsPerRowGrp];
		for (int i = 0; i < vals.length; i++)
			vals[i] = i;
		return vals;
	}

	private static MessageType getMessageType()
	{
		List<Type> parquetCols = asList(//
				new PrimitiveType(OPTIONAL, INT32, "int32"),//
				new PrimitiveType(OPTIONAL, INT32, "int32_rev"),//
				new PrimitiveType(OPTIONAL, INT64, "int64"),//
				new PrimitiveType(OPTIONAL, BINARY, "binary"),//
				new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 1, "fixed_bin"),//
				new PrimitiveType(OPTIONAL, DOUBLE, "double"), //
				new PrimitiveType(OPTIONAL, FLOAT, "float"),//
				new PrimitiveType(OPTIONAL, BOOLEAN, "boolean"));

		MessageType messageType = new MessageType("root", parquetCols);
		return messageType;
	}
}
