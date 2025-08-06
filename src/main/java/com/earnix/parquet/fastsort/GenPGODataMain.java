package com.earnix.parquet.fastsort;

import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import org.apache.commons.io.file.PathUtils;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;

/**
 * A class with a main method that generates profiler data for the graalvm optimizer. This class generates a parquet
 * file with all column types, and sorts by different column types so graalvm native-image pgo can make better
 * optimizing decisions.
 */
public class GenPGODataMain
{
	private static final Random random = new Random(1);
	private static final int numRowGroups = 5;
	private static final int numRowsPerRowGrp = 30_000;
	private static final int numPerDictCol = 200;

	public static void main(String[] args) throws IOException
	{
		System.out.println("Running sample sort application to generate pgo data");
		Path outDir = Files.createTempDirectory("parquet_tmp");
		try
		{
			generatePgoData(outDir);
		}
		finally
		{
			PathUtils.deleteDirectory(outDir);
		}
	}

	private static void generatePgoData(Path outDir) throws IOException
	{
		MessageType messageType = getMessageType();
		for (CompressionCodec compression : List.of(CompressionCodec.ZSTD, CompressionCodec.SNAPPY,
				CompressionCodec.UNCOMPRESSED))
		{
			Path outFileUnsorted = outDir.resolve("test_" + compression + ".parquet");
			try (var writer = ParquetFileColumnarWriterFactory.createWriter(outFileUnsorted, messageType, compression,
					true))
			{
				for (int rgIdx = 0; rgIdx < numRowGroups; rgIdx++)
				{
					writer.writeRowGroup(numRowsPerRowGrp, rga -> {
						var it = messageType.getColumns().iterator();
						rga.writeValues(
								chunkWriter -> chunkWriter.writeColumn(it.next(), makeRandIntData(numRowsPerRowGrp)));
						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeDictIntCol(numPerDictCol, numRowsPerRowGrp)));
						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeRangeIntCol(numPerDictCol, numRowsPerRowGrp)));

						rga.writeValues(
								chunkWriter -> chunkWriter.writeColumn(it.next(), makeRandLongData(numRowsPerRowGrp)));
						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeDictLongCol(numPerDictCol, numRowsPerRowGrp)));

						rga.writeValues(chunkWriter -> chunkWriter.writeBinaryColumnBytes(it.next(),
								List.of(makeRandBinColumn(numRowsPerRowGrp)).iterator()));
						rga.writeValues(chunkWriter -> chunkWriter.writeBinaryColumnBytes(it.next(),
								List.of(makeDictBinColumn(numPerDictCol, numRowsPerRowGrp)).iterator()));

						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeRandDoubleData(numRowsPerRowGrp)));
						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeDictDoubleCol(numPerDictCol, numRowsPerRowGrp)));

						rga.writeValues(
								chunkWriter -> chunkWriter.writeColumn(it.next(), makeRandFloatData(numRowsPerRowGrp)));
						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeDictFloatCol(numPerDictCol, numRowsPerRowGrp)));

						rga.writeValues(chunkWriter -> chunkWriter.writeColumn(it.next(),
								makeRandBooleanData(numRowsPerRowGrp)));
					});
				}
				writer.finishAndWriteFooterMetadata();
			}

			// Sort by each column type so graalvm gets pgo for all column types.
			for (var cd : messageType.getColumns())
			{
				Main.runSort(outFileUnsorted, cd.getPath()[0], outFileUnsorted.resolveSibling("out_sorted.parquet"));
			}
		}
	}

	/**
	 * @return sample schema for a variety of column types and data distributions to trigger pgo data for many column
	 * 		encodings
	 */
	private static MessageType getMessageType()
	{
		List<Type> parquetCols = asList(
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "random_int"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "dict_int"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "range_int"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "random_long"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "dict_long"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "random_bin"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "dict_bin"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "random_double"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "dict_double"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "random_float"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "dict_float"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "boolean"));

		MessageType messageType = new MessageType("root", parquetCols);
		return messageType;
	}

	private static int[] makeRandIntData(int numElements)
	{
		return random.ints(numElements).toArray();
	}

	private static int[] makeDictIntCol(int numVals, int numElements)
	{
		int[] dict = makeRandIntData(numVals);
		return random.ints(numElements, 0, numVals)//
				.map(i -> dict[i]).toArray();
	}

	private static float[] makeRandFloatData(int numElements)
	{
		float[] ret = new float[numElements];
		for (int i = 0; i < numElements; i++)
			ret[i] = random.nextFloat();
		return ret;
	}

	private static float[] makeDictFloatCol(int numVals, int numElements)
	{
		float[] dict = makeRandFloatData(numVals);
		float[] ret = new float[numElements];
		for (int i = 0; i < ret.length; i++)
			ret[i] = dict[random.nextInt(dict.length)];
		return ret;
	}

	private static boolean[] makeRandBooleanData(int numElements)
	{
		boolean[] ret = new boolean[numElements];
		for (int i = 0; i < numElements; i++)
			ret[i] = random.nextBoolean();
		return ret;
	}

	private static int[] makeRangeIntCol(int numVals, int numElements)
	{
		return random.ints(numElements, 0, numVals).toArray();
	}

	private static double[] makeRandDoubleData(int numElements)
	{
		return random.doubles(numElements).toArray();
	}

	private static double[] makeDictDoubleCol(int numVals, int numElements)
	{
		double[] dict = makeRandDoubleData(numVals);
		return random.ints(numElements, 0, numVals)//
				.mapToDouble(i -> dict[i]).toArray();
	}

	private static long[] makeRandLongData(int numElements)
	{
		return random.longs(numElements).toArray();
	}

	private static long[] makeDictLongCol(int numVals, int numElements)
	{
		long[] dict = random.longs(numVals).toArray();
		return random.ints(numElements, 0, numVals)//
				.mapToLong(i -> dict[i]).toArray();
	}

	private static byte[][] makeRandBinColumn(int numElements)
	{
		return random.longs(numElements)//
				.mapToObj(Long::toString)//
				.map(s -> s.getBytes(StandardCharsets.US_ASCII))//
				.toArray(byte[][]::new);
	}

	private static byte[][] makeDictBinColumn(int numVals, int numElements)
	{
		byte[][] dict = makeRandBinColumn(numVals);
		return random.ints(numElements, 0, numVals).mapToObj(i -> dict[i]).toArray(byte[][]::new);
	}
}
