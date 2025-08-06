package com.earnix.parquet.fastsort;

import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileInfo;
import com.earnix.parquet.columnar.writer.ParquetWriterUtils;
import com.earnix.parquet.fastsort.primitives.DoubleVals;
import com.earnix.parquet.fastsort.primitives.IntVals;
import com.earnix.parquet.fastsort.primitives.LongVals;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.function.Uncheck;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static com.earnix.parquet.fastsort.ColumnReader.NULL_BYTE;
import static com.earnix.parquet.fastsort.ColumnReader.TRUE_BYTE;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;

/**
 * A class that handles writing to many row groups at the same time column by column, and performs a merge of the files
 * at the end
 */
public class RowGrpWriters
{
	// the paths of each of the rowgroup files, index is the row group number
	private final Path[] paths;
	// writers for each of the rowgroup files, index is the row group number
	private final ParquetColumnarWriter[] writers;
	// the number of rows in each row group
	private final long[] rowsPerRg;
	// the starting index of each rowgroup.
	private final long[] rgStartIdx;

	public RowGrpWriters(Path tmpDir, MessageType messageType, long[] rowsPerRowgroup) throws IOException
	{
		this.rowsPerRg = rowsPerRowgroup;
		writers = new ParquetColumnarWriter[rowsPerRowgroup.length];
		this.paths = new Path[rowsPerRowgroup.length];
		for (int i = 0; i < writers.length; i++)
		{
			Path rgOutFile = paths[i] = tmpDir.resolve("rg_" + i + ".parquet");
			writers[i] = ParquetFileColumnarWriterFactory.createWriter(rgOutFile, messageType, CompressionCodec.SNAPPY,
					false);
			writers[i].startNewRowGroup(rowsPerRowgroup[i]);
		}

		this.rgStartIdx = new long[rowsPerRowgroup.length];
		for (int i = 1; i < rowsPerRowgroup.length; i++)
		{
			rgStartIdx[i] = rgStartIdx[i - 1] + rowsPerRg[i - 1];
		}
	}

	/**
	 * Write an integer column from the values store in parallel
	 *
	 * @param descriptor the column descriptor
	 * @param vals       the values to write (in order)
	 */
	public void writeIntCol(ColumnDescriptor descriptor, IntVals vals)
	{
		IntStream.range(0, writers.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> writers[rgIdx].getCurrentRowGroupWriter()
					.writeValues(f -> f.writeColumn(descriptor, vals.iterator(start(rgIdx), end(rgIdx)))));
		});
	}

	/**
	 * Write a float column from the values store in parallel
	 *
	 * @param descriptor the column descriptor
	 * @param vals       the values to write (in order)
	 */
	public void writeFloatCol(ColumnDescriptor descriptor, IntVals vals)
	{
		IntStream.range(0, writers.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> writers[rgIdx].getCurrentRowGroupWriter()
					.writeValues(f -> f.writeColumn(descriptor, vals.floatIterator(start(rgIdx), end(rgIdx)))));
		});
	}

	/**
	 * Write a long column from the values store in parallel
	 *
	 * @param descriptor the column descriptor
	 * @param vals       the values to write (in order)
	 */
	public void writeLongCol(ColumnDescriptor descriptor, LongVals vals)
	{
		IntStream.range(0, writers.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> writers[rgIdx].getCurrentRowGroupWriter()
					.writeValues(f -> f.writeColumn(descriptor, vals.iterator(start(rgIdx), end(rgIdx)))));
		});
	}

	/**
	 * Write a double column from the values store in parallel
	 *
	 * @param descriptor the column descriptor
	 * @param vals       the values to write (in order)
	 */
	public void writeDoubleCol(ColumnDescriptor descriptor, DoubleVals vals)
	{
		IntStream.range(0, writers.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> writers[rgIdx].getCurrentRowGroupWriter()
					.writeValues(f -> f.writeColumn(descriptor, vals.iterator(start(rgIdx), end(rgIdx)))));
		});
	}

	private int start(int rgIdx)
	{
		int ret = toIntExact(rgStartIdx[rgIdx]);
		return ret;
	}

	private int end(int rgIdx)
	{
		int ret = toIntExact(addExact(rgStartIdx[rgIdx], rowsPerRg[rgIdx]));
		return ret;
	}

	/**
	 * Write a boolean column from the values in parallel
	 *
	 * @param descriptor the column descriptor
	 * @param vals       the values to write (in order)
	 */
	public void writeBooleanCol(ColumnDescriptor descriptor, byte[] vals)
	{
		IntStream.range(0, writers.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> writers[rgIdx].getCurrentRowGroupWriter()
					.writeValues(f -> f.writeColumn(descriptor, new Iterator<>()
					{
						final int end = end(rgIdx);
						int currIdx = start(rgIdx);

						@Override
						public boolean hasNext()
						{
							return currIdx < end;
						}

						@Override
						public Boolean next()
						{
							if (!hasNext())
								throw new IllegalStateException();
							Boolean ret = vals[currIdx] == NULL_BYTE ? null : vals[currIdx] == TRUE_BYTE;
							currIdx++;
							return ret;
						}
					})));
		});
	}

	/**
	 * Write a binary boolean column from the values in parallel
	 *
	 * @param descriptor the column descriptor
	 * @param vals       the values to write (in order)
	 */
	public void writeBinaryCol(ColumnDescriptor descriptor, byte[][] vals)
	{
		List<byte[]> valsAsList = asList(vals);
		IntStream.range(0, writers.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> writers[rgIdx].getCurrentRowGroupWriter().writeValues(
					f -> f.writeBinaryColumnBytes(descriptor,
							valsAsList.subList(start(rgIdx), end(rgIdx)).iterator())));
		});
	}

	/**
	 * Finish all of the generated parquet row group files and merge them into a single parquet file in parallel
	 *
	 * @param mergeFile the file to merge all of the row groups into
	 * @throws IOException on failure to write (or read)
	 */
	public void finish(Path mergeFile) throws IOException
	{
		Stopwatch stopwatch = Stopwatch.createStarted();
		List<ParquetFileInfo> infos = finishAllTmpParquetFiles();
		System.out.println("Finishing open tmp rowgroup parquet files took " + stopwatch.elapsed());
		stopwatch.reset().start();

		try (var out = FileChannel.open(mergeFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING))
		{
			ParquetMagicUtils.writeMagicBytes(out);
			long[] offsets = copyTmpFilesIntoOutFile(infos, out);
			FileMetaData outMd = buildNewFooterMetadata(infos, offsets);
			out.position(offsets[offsets.length - 1]);
			ParquetWriterUtils.writeFooterMetadataAndMagic(out, outMd);
		}
		System.out.println("Copying tmp files into destination file took " + stopwatch.elapsed());
	}

	private FileMetaData buildNewFooterMetadata(List<ParquetFileInfo> infos, long[] offsets)
	{
		long totalRows = 0;
		List<RowGroup> rowGroups = new ArrayList<>(writers.length);

		var it = infos.iterator();
		for (int rgIdx = 0; it.hasNext(); rgIdx++)
		{
			var info = it.next();
			totalRows += info.getFileMetaData().getNum_rows();
			RowGroup rg = info.getFileMetaData().getRow_groups().getFirst();
			if (rg.isSetFile_offset())
				rg.setFile_offset(rg.getFile_offset() + offsets[rgIdx]);

			for (var col : rg.getColumns())
			{
				if (col.getMeta_data().isSetDictionary_page_offset())
				{
					col.getMeta_data()
							.setDictionary_page_offset(col.getMeta_data().getDictionary_page_offset() + offsets[rgIdx]);
				}
				col.getMeta_data().setData_page_offset(col.getMeta_data().getData_page_offset() + offsets[rgIdx]);
			}

			rowGroups.add(rg);
		}
		FileMetaData outMd = infos.getFirst().getFileMetaData();
		outMd.setNum_rows(totalRows);
		outMd.setRow_groups(rowGroups);
		return outMd;
	}

	private long[] copyTmpFilesIntoOutFile(List<ParquetFileInfo> infos, FileChannel out)
	{
		final long startOffset = ParquetMagicUtils.PARQUET_MAGIC.length();
		long[] offsets = computeDstOffsets(infos, startOffset);

		IntStream.range(0, paths.length).parallel().forEach(rgIdx -> {
			Uncheck.run(() -> {
				try (var in = FileChannel.open(paths[rgIdx], StandardOpenOption.READ))
				{
					final long count = offsets[rgIdx + 1] - offsets[rgIdx];
					in.position(startOffset);
					out.transferFrom(in, startOffset + offsets[rgIdx], count);
				}
				Files.delete(paths[rgIdx]);
			});
		});
		return offsets;
	}

	private List<ParquetFileInfo> finishAllTmpParquetFiles() throws IOException
	{
		try
		{
			List<ParquetFileInfo> infos = Arrays.stream(writers).parallel().map(writer -> Uncheck.get(() -> {
				writer.finishRowGroup();
				var info = writer.finishAndWriteFooterMetadata();
				return info;
			})).toList();
			return infos;
		}
		finally
		{
			close();
		}
	}

	private long[] computeDstOffsets(List<ParquetFileInfo> infos, long startOffset)
	{
		long[] offsets = new long[paths.length + 1];
		for (int i = 0; i < infos.size(); i++)
		{
			offsets[i + 1] = offsets[i] + infos.get(i).getFooterMetadataStartOffset() - startOffset;
		}
		return offsets;
	}

	public void close() throws IOException
	{
		IOException ex = null;
		for (var writer : writers)
		{
			try
			{
				writer.close();
			}
			catch (IOException e)
			{
				if (ex == null)
					ex = e;
			}
		}
		if (ex != null)
			throw ex;
	}
}
