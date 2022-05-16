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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.LongValueStatisticsBuilder;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.LongOutputStreamDwrf;
import com.facebook.presto.orc.stream.LongOutputStreamV2;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.writer.ColumnWriterUtils.buildRowGroupIndexes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LongColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongColumnWriter.class).instanceSize();
    private final int column;
    private final int sequence;
    private final Type type;
    private final long typeSize;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream dataStream;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private final CompressedMetadataWriter metadataWriter;
    private long columnStatisticsRetainedSizeInBytes;
    private PresentOutputStream presentStream;

    private final Supplier<LongValueStatisticsBuilder> statisticsBuilderSupplier;
    private LongValueStatisticsBuilder statisticsBuilder;

    private boolean closed;

    public LongColumnWriter(
            int column,
            int sequence,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            Supplier<LongValueStatisticsBuilder> statisticsBuilderSupplier,
            MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(sequence >= 0, "sequence is negative");
        checkArgument(type instanceof FixedWidthType, "Type is not instance of FixedWidthType");
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        requireNonNull(metadataWriter, "metadataWriter is null");

        this.column = column;
        this.sequence = sequence;
        this.type = requireNonNull(type, "type is null");
        this.typeSize = ((FixedWidthType) type).getFixedSize();
        this.compressed = columnWriterOptions.getCompressionKind() != NONE;
        if (orcEncoding == DWRF) {
            this.columnEncoding = new ColumnEncoding(DIRECT, 0);
            this.dataStream = new LongOutputStreamDwrf(columnWriterOptions, dwrfEncryptor, true, DATA);
        }
        else {
            this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
            this.dataStream = new LongOutputStreamV2(columnWriterOptions, true, DATA);
        }
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.statisticsBuilderSupplier = requireNonNull(statisticsBuilderSupplier, "statisticsBuilderSupplier is null");
        this.statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(column, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        beginDataRowGroup();
    }

    void beginDataRowGroup()
    {
        dataStream.recordCheckpoint();
    }

    void updatePresentStream(PresentOutputStream updatedPresentStream)
    {
        requireNonNull(updatedPresentStream, "updatedPresentStream is null");
        checkState(presentStream.getBufferedBytes() == 0, "Present stream has some content");
        presentStream = updatedPresentStream;
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        int nonNullValueCount = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                long value = type.getLong(block, position);
                writeValue(value);
                nonNullValueCount++;
            }
        }
        return nonNullValueCount * typeSize + (block.getPositionCount() - nonNullValueCount) * NULL_SIZE;
    }

    void writeValue(long value)
    {
        dataStream.writeLong(value);
        statisticsBuilder.addValue(value);
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        rowGroupColumnStatistics.add(statistics);
        columnStatisticsRetainedSizeInBytes += statistics.getRetainedSizeInBytes();
        statisticsBuilder = statisticsBuilderSupplier.get();
        return ImmutableMap.of(column, statistics);
    }

    @Override
    public void close()
    {
        closed = true;
        dataStream.close();
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<? extends StreamCheckpoint>> prependCheckpoints)
            throws IOException
    {
        checkState(closed);

        List<RowGroupIndex> rowGroupIndexes = buildRowGroupIndexes(compressed, rowGroupColumnStatistics, prependCheckpoints, presentStream, dataStream);
        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes);
        Stream stream = new Stream(column, sequence, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column, sequence).ifPresent(outputDataStreams::add);
        outputDataStreams.add(dataStream.getStreamDataOutput(column, sequence));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return dataStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + dataStream.getRetainedBytes() + presentStream.getRetainedBytes() + columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        dataStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();
    }
}
