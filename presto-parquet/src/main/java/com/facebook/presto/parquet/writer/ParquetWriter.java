/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.writer;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.writer.ColumnWriter.BufferData;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.facebook.presto.parquet.writer.ParquetDataOutput.createDataOutput;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.BROTLI;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZO;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;

public class ParquetWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ParquetWriter.class).instanceSize();

    private static final int CHUNK_MAX_BYTES = toIntExact(DataSize.valueOf("128MB").toBytes());
    private static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;

    private final List<ColumnWriter> columnWriters;
    private final OutputStreamSliceOutput outputStream;
    private final List<Type> types;
    private final ParquetWriterOptions writerOption;
    private final List<String> names;
    private final MessageType messageType;

    private final int chunkMaxLogicalBytes;

    private ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();

    private int rows;
    private boolean closed;
    private boolean writeHeader;

    public static final Slice MAGIC = wrappedBuffer("PAR1".getBytes(US_ASCII));

    public ParquetWriter(OutputStream outputStream, List<String> columnNames, List<Type> types, ParquetWriterOptions writerOption, String compressionCodecClass)
    {
        this.outputStream = new OutputStreamSliceOutput(requireNonNull(outputStream, "outputstream is null"));
        this.names = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.writerOption = requireNonNull(writerOption, "writerOption is null");

        checkArgument(types.size() == columnNames.size(), "type size %s is not equal to name size %s", types.size(), columnNames.size());

        ParquetSchemaConverter parquetSchemaConverter = new ParquetSchemaConverter(types, columnNames);
        this.messageType = parquetSchemaConverter.getMessageType();

        ParquetProperties parquetProperties = ParquetProperties.builder()
                .withWriterVersion(PARQUET_2_0)
                .withPageSize(writerOption.getMaxPageSize())
                .build();
        CompressionCodecName compressionCodecName = getCompressionCodecName(compressionCodecClass);
        this.columnWriters = ParquetWriters.getColumnWriters(messageType, parquetSchemaConverter.getPrimitiveTypes(), parquetProperties, compressionCodecName);

        this.chunkMaxLogicalBytes = max(1, CHUNK_MAX_BYTES / 2);
    }

    public long getWrittenBytes()
    {
        return outputStream.size();
    }

    public long getBufferedBytes()
    {
        return columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum();
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                outputStream.getRetainedSize() +
                columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum();
    }

    public void write(Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        checkState(!closed, "writer is closed");
        if (page.getPositionCount() == 0) {
            return;
        }

        checkArgument(page.getChannelCount() == columnWriters.size());

        while (page != null) {
            int chunkRows = min(page.getPositionCount(), DEFAULT_ROW_GROUP_MAX_ROW_COUNT);
            Page chunk = page.getRegion(0, chunkRows);

            // avoid chunk with huge logical size
            while (chunkRows > 1 && chunk.getLogicalSizeInBytes() > chunkMaxLogicalBytes) {
                chunkRows /= 2;
                chunk = chunk.getRegion(0, chunkRows);
            }

            // Remove chunk from current page
            if (chunkRows < page.getPositionCount()) {
                page = page.getRegion(chunkRows, page.getPositionCount() - chunkRows);
            }
            else {
                page = null;
            }

            writeChunk(chunk);
        }
    }

    private void writeChunk(Page page)
            throws IOException
    {
        long bufferedBytes = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            ColumnWriter writer = columnWriters.get(channel);
            writer.writeBlock(new ColumnChunk(page.getBlock(channel)));
            bufferedBytes += writer.getBufferedBytes();
        }
        rows += page.getPositionCount();

        if (bufferedBytes >= writerOption.getMaxRowGroupSize()) {
            columnWriters.forEach(ColumnWriter::close);
            flush();
            columnWriters.forEach(ColumnWriter::reset);
            rows = 0;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        columnWriters.forEach(ColumnWriter::close);

        flush();
        writeFooter();
        outputStream.close();
    }

    // Parquet File Layout:
    //
    // MAGIC
    // variable: Data
    // variable: Metadata
    // 4 bytes: MetadataLength
    // MAGIC
    private void flush()
            throws IOException
    {
        // write header
        if (!writeHeader) {
            createDataOutput(MAGIC).writeData(outputStream);
            writeHeader = true;
        }

        // get all data in buffer
        ImmutableList.Builder<BufferData> builder = ImmutableList.builder();
        for (ColumnWriter columnWriter : columnWriters) {
            columnWriter.getBuffer().forEach(builder::add);
        }
        List<BufferData> bufferDataList = builder.build();

        // update stats
        long stripeStartOffset = outputStream.size();
        List<ColumnMetaData> metadatas = bufferDataList.stream()
                .map(BufferData::getMetaData)
                .collect(toImmutableList());
        updateRowGroups(updateColumnMetadataOffset(metadatas, stripeStartOffset));

        // flush pages
        bufferDataList.stream()
                .map(BufferData::getData)
                .flatMap(List::stream)
                .forEach(data -> data.writeData(outputStream));
    }

    private void writeFooter()
            throws IOException
    {
        checkState(closed);
        Slice footer = getFooter(rowGroupBuilder.build(), messageType);
        createDataOutput(footer).writeData(outputStream);

        Slice footerSize = Slices.allocate(SIZE_OF_INT);
        footerSize.setInt(0, footer.length());
        createDataOutput(footerSize).writeData(outputStream);

        createDataOutput(MAGIC).writeData(outputStream);
    }

    static Slice getFooter(List<RowGroup> rowGroups, MessageType messageType)
            throws IOException
    {
        FileMetaData fileMetaData = new FileMetaData();
        fileMetaData.setVersion(1);
        fileMetaData.setSchema(MessageTypeConverter.toParquetSchema(messageType));
        long totalRows = rowGroups.stream().mapToLong(RowGroup::getNum_rows).sum();
        fileMetaData.setNum_rows(totalRows);
        fileMetaData.setRow_groups(ImmutableList.copyOf(rowGroups));

        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(40);
        Util.writeFileMetaData(fileMetaData, dynamicSliceOutput);
        return dynamicSliceOutput.slice();
    }

    private void updateRowGroups(List<ColumnMetaData> columnMetaData)
    {
        // TODO Avoid writing empty row group
        long totalBytes = columnMetaData.stream().mapToLong(ColumnMetaData::getTotal_compressed_size).sum();
        ImmutableList<org.apache.parquet.format.ColumnChunk> columnChunks = columnMetaData.stream().map(ParquetWriter::toColumnChunk).collect(toImmutableList());
        rowGroupBuilder.add(new RowGroup(columnChunks, totalBytes, rows));
    }

    private static org.apache.parquet.format.ColumnChunk toColumnChunk(ColumnMetaData metaData)
    {
        // TODO Not sure whether file_offset is used
        org.apache.parquet.format.ColumnChunk columnChunk = new org.apache.parquet.format.ColumnChunk(0);
        columnChunk.setMeta_data(metaData);
        return columnChunk;
    }

    private List<ColumnMetaData> updateColumnMetadataOffset(List<ColumnMetaData> columns, long offset)
    {
        ImmutableList.Builder<ColumnMetaData> builder = ImmutableList.builder();
        long currentOffset = offset;
        for (ColumnMetaData column : columns) {
            ColumnMetaData columnMetaData = new ColumnMetaData(column.type, column.encodings, column.path_in_schema, column.codec, column.num_values, column.total_uncompressed_size, column.total_compressed_size, currentOffset);
            columnMetaData.setStatistics(column.getStatistics());
            builder.add(columnMetaData);
            currentOffset += column.getTotal_compressed_size();
        }
        return builder.build();
    }

    private CompressionCodecName getCompressionCodecName(String compressionCodecClass)
    {
        if (compressionCodecClass == null) {
            return UNCOMPRESSED;
        }
        else if (compressionCodecClass.equals("parquet.hadoop.codec.SnappyCodec") || compressionCodecClass.equals("org.apache.parquet.hadoop.codec.SnappyCodec")) {
            return SNAPPY;
        }
        else if (compressionCodecClass.equals("org.apache.hadoop.io.compress.GzipCodec")) {
            return GZIP;
        }
        else if (compressionCodecClass.equals("com.hadoop.compression.lzo.LzoCodec")) {
            return LZO;
        }
        else if (compressionCodecClass.equals("org.apache.hadoop.io.compress.BrotliCodec")) {
            return BROTLI;
        }
        else if (compressionCodecClass.equals("org.apache.hadoop.io.compress.Lz4Codec")) {
            return LZ4;
        }
        else if (compressionCodecClass.equals("org.apache.hadoop.io.compress.ZStandardCodec")) {
            return ZSTD;
        }
        throw new IllegalArgumentException("Invalid compressionCodec: " + compressionCodecClass);
    }
}
