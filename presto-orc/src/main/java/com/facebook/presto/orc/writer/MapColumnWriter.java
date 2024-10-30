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
import com.facebook.presto.common.block.ColumnarMap;
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
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.facebook.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.facebook.presto.orc.writer.ColumnWriterUtils.buildRowGroupIndexes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MapColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapColumnWriter.class).instanceSize();
    private final int column;
    private final int sequence;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream lengthStream;
    private final PresentOutputStream presentStream;
    private final CompressedMetadataWriter metadataWriter;
    private final ColumnWriter keyWriter;
    private final ColumnWriter valueWriter;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;

    private int nonNullValueCount;
    private long rawSize;

    private boolean closed;

    public MapColumnWriter(
            int column,
            int sequence,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            ColumnWriter keyWriter,
            ColumnWriter valueWriter,
            MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(sequence >= 0, "sequence is negative");
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        this.column = column;
        this.sequence = sequence;
        this.compressed = columnWriterOptions.getCompressionKind() != NONE;
        this.columnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DIRECT : DIRECT_V2, 0);
        this.keyWriter = requireNonNull(keyWriter, "keyWriter is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
        this.lengthStream = createLengthOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.<ColumnWriter>builder()
                .add(keyWriter)
                .addAll(keyWriter.getNestedColumnWriters())
                .add(valueWriter)
                .addAll(valueWriter.getNestedColumnWriters())
                .build();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(column, columnEncoding);
        encodings.putAll(keyWriter.getColumnEncodings());
        encodings.putAll(valueWriter.getColumnEncodings());
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        lengthStream.recordCheckpoint();
        presentStream.recordCheckpoint();

        keyWriter.beginRowGroup();
        valueWriter.beginRowGroup();
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarMap columnarMap = toColumnarMap(block);
        return writeColumnarMap(columnarMap);
    }

    private long writeColumnarMap(ColumnarMap columnarMap)
    {
        // write nulls and lengths
        int blockNonNullValueCount = 0;
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            boolean present = !columnarMap.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                blockNonNullValueCount++;
                lengthStream.writeLong(columnarMap.getEntryCount(position));
            }
        }

        // write keys and value
        Block keysBlock = columnarMap.getKeysBlock();
        long childRawSize = 0;
        if (keysBlock.getPositionCount() > 0) {
            childRawSize += keyWriter.writeBlock(keysBlock);
            childRawSize += valueWriter.writeBlock(columnarMap.getValuesBlock());
        }
        nonNullValueCount += blockNonNullValueCount;
        long rawSize = (columnarMap.getPositionCount() - blockNonNullValueCount) * NULL_SIZE + childRawSize;
        this.rawSize += rawSize;
        return rawSize;
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);

        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, null, rawSize, null);
        rowGroupColumnStatistics.add(statistics);
        columnStatisticsRetainedSizeInBytes += statistics.getRetainedSizeInBytes();
        nonNullValueCount = 0;
        rawSize = 0;

        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(column, statistics);
        columnStatistics.putAll(keyWriter.finishRowGroup());
        columnStatistics.putAll(valueWriter.finishRowGroup());
        return columnStatistics.build();
    }

    @Override
    public void close()
    {
        closed = true;
        keyWriter.close();
        valueWriter.close();
        lengthStream.close();
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        columnStatistics.putAll(keyWriter.getColumnStripeStatistics());
        columnStatistics.putAll(valueWriter.getColumnStripeStatistics());
        return columnStatistics.build();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<? extends StreamCheckpoint>> prependCheckpoints)
            throws IOException
    {
        checkState(closed);

        List<RowGroupIndex> rowGroupIndexes = buildRowGroupIndexes(compressed, rowGroupColumnStatistics, prependCheckpoints, presentStream, lengthStream);
        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes);
        Stream stream = new Stream(column, sequence, StreamKind.ROW_INDEX, slice.length(), false);

        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));
        indexStreams.addAll(keyWriter.getIndexStreams(Optional.empty()));
        indexStreams.addAll(valueWriter.getIndexStreams(Optional.empty()));
        return indexStreams.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column, sequence).ifPresent(outputDataStreams::add);
        outputDataStreams.add(lengthStream.getStreamDataOutput(column, sequence));
        outputDataStreams.addAll(keyWriter.getDataStreams());
        outputDataStreams.addAll(valueWriter.getDataStreams());
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return lengthStream.getBufferedBytes() + presentStream.getBufferedBytes() + keyWriter.getBufferedBytes() + valueWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + lengthStream.getRetainedBytes() + presentStream.getRetainedBytes() + keyWriter.getRetainedBytes() + valueWriter.getRetainedBytes() + columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        lengthStream.reset();
        presentStream.reset();
        keyWriter.reset();
        valueWriter.reset();
        rowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        nonNullValueCount = 0;
        rawSize = 0;
    }
}
