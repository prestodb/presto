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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterable;
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterables;
import com.facebook.presto.parquet.writer.levels.RepetitionLevelIterable;
import com.facebook.presto.parquet.writer.levels.RepetitionLevelIterables;
import com.facebook.presto.parquet.writer.valuewriter.PrimitiveValueWriter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.parquet.writer.ParquetCompressor.getCompressor;
import static com.facebook.presto.parquet.writer.ParquetDataOutput.createDataOutput;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.bytes.BytesInput.copy;

public abstract class PrimitiveColumnWriter
        implements ColumnWriter
{
    protected final ColumnDescriptor columnDescriptor;
    protected final PrimitiveValueWriter primitiveValueWriter;
    protected final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    protected final Set<Encoding> encodings;
    protected final int maxDefinitionLevel;
    protected final List<ParquetDataOutput> pageBuffer = new ArrayList<>();

    @Nullable
    protected final ParquetCompressor compressor;
    protected final int pageSizeThreshold;

    private final Type type;
    private final CompressionCodecName compressionCodec;

    protected boolean closed;
    protected boolean getDataStreamsCalled;

    // current page stats
    protected int valueCount;
    protected int currentPageNullCounts;

    // column meta data stats
    protected long totalCompressedSize;
    protected long totalUnCompressedSize;
    protected long totalValues;
    protected Statistics<?> columnStatistics;

    public PrimitiveColumnWriter(Type type, ColumnDescriptor columnDescriptor, PrimitiveValueWriter primitiveValueWriter, CompressionCodecName compressionCodecName, int pageSizeThreshold)
    {
        this.type = requireNonNull(type, "type is null");
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor is null");
        this.maxDefinitionLevel = columnDescriptor.getMaxDefinitionLevel();

        this.primitiveValueWriter = requireNonNull(primitiveValueWriter, "primitiveValueWriter is null");
        this.encodings = new HashSet<>();
        this.compressionCodec = requireNonNull(compressionCodecName, "compressionCodecName is null");
        this.compressor = getCompressor(compressionCodecName);
        this.pageSizeThreshold = pageSizeThreshold;

        this.columnStatistics = Statistics.createStats(columnDescriptor.getPrimitiveType());
    }

    @Override
    public void writeBlock(ColumnChunk columnChunk)
            throws IOException
    {
        checkState(!closed);

        ColumnChunk current = new ColumnChunk(columnChunk.getBlock(),
                ImmutableList.<DefinitionLevelIterable>builder()
                        .addAll(columnChunk.getDefinitionLevelIterables())
                        .add(DefinitionLevelIterables.of(columnChunk.getBlock(), maxDefinitionLevel))
                        .build(),
                ImmutableList.<RepetitionLevelIterable>builder()
                        .addAll(columnChunk.getRepetitionLevelIterables())
                        .add(RepetitionLevelIterables.of(columnChunk.getBlock()))
                        .build());

        // write values
        primitiveValueWriter.write(columnChunk.getBlock());

        writeDefinitionAndRepetitionLevels(current);

        if (getBufferedBytes() >= pageSizeThreshold) {
            flushCurrentPageToBuffer();
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public List<BufferData> getBuffer()
            throws IOException
    {
        checkState(closed);
        return ImmutableList.of(new BufferData(getDataStreams(), getColumnMetaData()));
    }

    // Returns ColumnMetaData that offset is invalid
    protected ColumnMetaData getColumnMetaData()
    {
        checkState(getDataStreamsCalled);

        ColumnMetaData columnMetaData = new ColumnMetaData(
                ParquetTypeConverter.getType(columnDescriptor.getPrimitiveType().getPrimitiveTypeName()),
                encodings.stream().map(parquetMetadataConverter::getEncoding).collect(toImmutableList()),
                ImmutableList.copyOf(columnDescriptor.getPath()),
                compressionCodec.getParquetCompressionCodec(),
                totalValues,
                totalUnCompressedSize,
                totalCompressedSize,
                -1);
        columnMetaData.setStatistics(ParquetMetadataConverter.toParquetStatistics(columnStatistics));
        return columnMetaData;
    }

    protected List<ParquetDataOutput> getDataStreams()
            throws IOException
    {
        List<ParquetDataOutput> dictPage = new ArrayList<>();
        if (valueCount > 0) {
            flushCurrentPageToBuffer();
        }
        // write dict page if possible
        DictionaryPage dictionaryPage = primitiveValueWriter.toDictPageAndClose();
        if (dictionaryPage != null) {
            BytesInput pageBytes = copy(dictionaryPage.getBytes());
            long uncompressedSize = dictionaryPage.getUncompressedSize();

            ParquetDataOutput pageData = createDataOutput(pageBytes);
            if (compressor != null) {
                pageData = compressor.compress(pageBytes);
            }
            long compressedSize = pageData.size();

            ByteArrayOutputStream dictStream = new ByteArrayOutputStream();
            parquetMetadataConverter.writeDictionaryPageHeader(toIntExact(uncompressedSize),
                    toIntExact(compressedSize),
                    dictionaryPage.getDictionarySize(),
                    dictionaryPage.getEncoding(),
                    dictStream);
            ParquetDataOutput pageHeader = createDataOutput(Slices.wrappedBuffer(dictStream.toByteArray()));
            dictPage.add(pageHeader);
            dictPage.add(pageData);
            totalCompressedSize += pageHeader.size() + compressedSize;
            totalUnCompressedSize += pageHeader.size() + uncompressedSize;

            primitiveValueWriter.resetDictionary();
        }
        getDataStreamsCalled = true;

        return ImmutableList.<ParquetDataOutput>builder()
                .addAll(dictPage)
                .addAll(pageBuffer)
                .build();
    }

    public abstract long getBufferedBytes();

    @Override
    public long getRetainedBytes()
    {
        return 0;
    }

    @Override
    public void resetChunk()
    {
        pageBuffer.clear();
        primitiveValueWriter.resetChunk();
        closed = false;

        totalCompressedSize = 0;
        totalUnCompressedSize = 0;
        totalValues = 0;
        encodings.clear();
        this.columnStatistics = Statistics.createStats(columnDescriptor.getPrimitiveType());

        getDataStreamsCalled = false;
    }

    protected abstract void writeDefinitionAndRepetitionLevels(ColumnChunk current)
            throws IOException;

    // page header
    // repetition levels
    // definition levels
    // data
    protected abstract void flushCurrentPageToBuffer()
            throws IOException;
}
