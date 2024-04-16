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
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.parquet.writer.ParquetDataOutput.createDataOutput;
import static com.facebook.presto.parquet.writer.levels.RepetitionLevelIterables.getIterator;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.bytes.BytesInput.copy;

public class PrimitiveColumnWriterV1
        extends PrimitiveColumnWriter
{
    private final ValuesWriter definitionLevelWriter;
    private final ValuesWriter repetitionLevelWriter;

    public PrimitiveColumnWriterV1(Type type, ColumnDescriptor columnDescriptor, PrimitiveValueWriter primitiveValueWriter, ValuesWriter definitionLevelWriter, ValuesWriter repetitionLevelWriter, CompressionCodecName compressionCodecName, int pageSizeThreshold)
    {
        super(type, columnDescriptor, primitiveValueWriter, compressionCodecName, pageSizeThreshold);

        this.definitionLevelWriter = requireNonNull(definitionLevelWriter, "definitionLevelWriter is null");
        this.repetitionLevelWriter = requireNonNull(repetitionLevelWriter, "repetitionLevelWriter is null");
    }

    protected void writeDefinitionAndRepetitionLevels(ColumnChunk current)
    {
        // write definition levels
        Iterator<Integer> defIterator = DefinitionLevelIterables.getIterator(current.getDefinitionLevelIterables());
        while (defIterator.hasNext()) {
            int next = defIterator.next();
            definitionLevelWriter.writeInteger(next);
            if (next != maxDefinitionLevel) {
                currentPageNullCounts++;
            }
            valueCount++;
        }

        // write repetition levels
        Iterator<Integer> repIterator = getIterator(current.getRepetitionLevelIterables());
        while (repIterator.hasNext()) {
            int next = repIterator.next();
            repetitionLevelWriter.writeInteger(next);
        }
    }

    // page header
    // repetition levels
    // definition levels
    // data
    protected void flushCurrentPageToBuffer()
            throws IOException
    {
        ImmutableList.Builder<ParquetDataOutput> outputDataStreams = ImmutableList.builder();

        BytesInput bytesInput = BytesInput.concat(copy(repetitionLevelWriter.getBytes()),
                copy(definitionLevelWriter.getBytes()),
                copy(primitiveValueWriter.getBytes()));
        ParquetDataOutput pageData = (compressor != null) ? compressor.compress(bytesInput) : createDataOutput(bytesInput);
        long uncompressedSize = bytesInput.size();
        long compressedSize = pageData.size();

        ByteArrayOutputStream pageHeaderOutputStream = new ByteArrayOutputStream();

        Statistics<?> statistics = primitiveValueWriter.getStatistics();
        statistics.incrementNumNulls(currentPageNullCounts);

        columnStatistics.mergeStatistics(statistics);

        parquetMetadataConverter.writeDataPageV1Header((int) uncompressedSize,
                (int) compressedSize,
                valueCount,
                repetitionLevelWriter.getEncoding(),
                definitionLevelWriter.getEncoding(),
                primitiveValueWriter.getEncoding(),
                pageHeaderOutputStream);

        ParquetDataOutput pageHeader = createDataOutput(Slices.wrappedBuffer(pageHeaderOutputStream.toByteArray()));
        outputDataStreams.add(pageHeader);
        outputDataStreams.add(pageData);

        List<ParquetDataOutput> dataOutputs = outputDataStreams.build();

        // update total stats
        totalUnCompressedSize += pageHeader.size() + uncompressedSize;
        totalCompressedSize += pageHeader.size() + compressedSize;
        totalValues += valueCount;

        pageBuffer.addAll(dataOutputs);

        // Add encoding should be called after ValuesWriter#getBytes() and before ValuesWriter#reset()
        encodings.add(repetitionLevelWriter.getEncoding());
        encodings.add(definitionLevelWriter.getEncoding());
        encodings.add(primitiveValueWriter.getEncoding());

        // reset page stats
        valueCount = 0;
        currentPageNullCounts = 0;

        repetitionLevelWriter.reset();
        definitionLevelWriter.reset();
        primitiveValueWriter.reset();
    }

    @Override
    public long getBufferedBytes()
    {
        return pageBuffer.stream().mapToLong(ParquetDataOutput::size).sum() +
                definitionLevelWriter.getBufferedSize() +
                repetitionLevelWriter.getBufferedSize() +
                primitiveValueWriter.getBufferedSize();
    }
}
