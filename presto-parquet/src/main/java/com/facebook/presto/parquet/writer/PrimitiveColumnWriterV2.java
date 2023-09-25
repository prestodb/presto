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
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterables;
import com.facebook.presto.parquet.writer.valuewriter.PrimitiveValueWriter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.parquet.writer.ParquetDataOutput.createDataOutput;
import static com.facebook.presto.parquet.writer.levels.RepetitionLevelIterables.getIterator;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.bytes.BytesInput.copy;

public class PrimitiveColumnWriterV2
        extends PrimitiveColumnWriter
{
    private final RunLengthBitPackingHybridEncoder definitionLevelEncoder;
    private final RunLengthBitPackingHybridEncoder repetitionLevelEncoder;

    // current page stats
    private int currentPageRowCount;

    public PrimitiveColumnWriterV2(Type type, ColumnDescriptor columnDescriptor, PrimitiveValueWriter primitiveValueWriter, RunLengthBitPackingHybridEncoder definitionLevelEncoder, RunLengthBitPackingHybridEncoder repetitionLevelEncoder, CompressionCodecName compressionCodecName, int pageSizeThreshold)
    {
        super(type, columnDescriptor, primitiveValueWriter, compressionCodecName, pageSizeThreshold);

        this.definitionLevelEncoder = requireNonNull(definitionLevelEncoder, "definitionLevelEncoder is null");
        this.repetitionLevelEncoder = requireNonNull(repetitionLevelEncoder, "repetitionLevelEncoder is null");
    }

    protected void writeDefinitionAndRepetitionLevels(ColumnChunk current)
            throws IOException
    {
        // write definition levels
        Iterator<Integer> defIterator = DefinitionLevelIterables.getIterator(current.getDefinitionLevelIterables());
        while (defIterator.hasNext()) {
            int next = defIterator.next();
            definitionLevelEncoder.writeInt(next);
            if (next != maxDefinitionLevel) {
                currentPageNullCounts++;
            }
            valueCount++;
        }

        // write repetition levels
        Iterator<Integer> repIterator = getIterator(current.getRepetitionLevelIterables());
        while (repIterator.hasNext()) {
            int next = repIterator.next();
            repetitionLevelEncoder.writeInt(next);
            if (next == 0) {
                currentPageRowCount++;
            }
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

    // page header
    // repetition levels
    // definition levels
    // data
    protected void flushCurrentPageToBuffer()
            throws IOException
    {
        ImmutableList.Builder<ParquetDataOutput> outputDataStreams = ImmutableList.builder();

        BytesInput bytes = primitiveValueWriter.getBytes();
        ParquetDataOutput repetitions = createDataOutput(copy(repetitionLevelEncoder.toBytes()));
        ParquetDataOutput definitions = createDataOutput(copy(definitionLevelEncoder.toBytes()));

        // Add encoding should be called after primitiveValueWriter.getBytes() and before primitiveValueWriter.reset()
        encodings.add(primitiveValueWriter.getEncoding());

        long uncompressedSize = bytes.size() + repetitions.size() + definitions.size();

        ParquetDataOutput data;
        long compressedSize;
        if (compressor != null) {
            data = compressor.compress(bytes);
            compressedSize = data.size() + repetitions.size() + definitions.size();
        }
        else {
            data = createDataOutput(copy(bytes));
            compressedSize = uncompressedSize;
        }

        ByteArrayOutputStream pageHeaderOutputStream = new ByteArrayOutputStream();

        Statistics<?> statistics = primitiveValueWriter.getStatistics();
        statistics.incrementNumNulls(currentPageNullCounts);

        columnStatistics.mergeStatistics(statistics);

        parquetMetadataConverter.writeDataPageV2Header((int) uncompressedSize,
                (int) compressedSize,
                valueCount,
                currentPageNullCounts,
                currentPageRowCount,
                statistics,
                primitiveValueWriter.getEncoding(),
                (int) repetitions.size(),
                (int) definitions.size(),
                pageHeaderOutputStream);

        ParquetDataOutput pageHeader = createDataOutput(Slices.wrappedBuffer(pageHeaderOutputStream.toByteArray()));
        outputDataStreams.add(pageHeader);
        outputDataStreams.add(repetitions);
        outputDataStreams.add(definitions);
        outputDataStreams.add(data);

        List<ParquetDataOutput> dataOutputs = outputDataStreams.build();

        // update total stats
        totalCompressedSize += pageHeader.size() + compressedSize;
        totalUnCompressedSize += pageHeader.size() + uncompressedSize;
        totalValues += valueCount;

        pageBuffer.addAll(dataOutputs);

        // reset page stats
        valueCount = 0;
        currentPageNullCounts = 0;
        currentPageRowCount = 0;

        definitionLevelEncoder.reset();
        repetitionLevelEncoder.reset();
        primitiveValueWriter.reset();
    }

    @Override
    public long getBufferedBytes()
    {
        return pageBuffer.stream().mapToLong(ParquetDataOutput::size).sum() +
                definitionLevelEncoder.getBufferedSize() +
                repetitionLevelEncoder.getBufferedSize() +
                primitiveValueWriter.getBufferedSize();
    }
}
