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
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.CompressionParameters;
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
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ListColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ListColumnWriter.class).instanceSize();
    private final int column;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream lengthStream;
    private final PresentOutputStream presentStream;
    private final CompressedMetadataWriter metadataWriter;
    private final ColumnWriter elementWriter;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;

    private int nonNullValueCount;

    private boolean closed;

    public ListColumnWriter(int column, CompressionParameters compressionParameters, Optional<DwrfDataEncryptor> dwrfEncryptor, OrcEncoding orcEncoding, ColumnWriter elementWriter, MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        requireNonNull(compressionParameters, "compressionParameters is null");
        this.column = column;
        this.compressed = compressionParameters.getKind() != NONE;
        this.columnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DIRECT : DIRECT_V2, 0);
        this.elementWriter = requireNonNull(elementWriter, "elementWriter is null");
        this.lengthStream = createLengthOutputStream(compressionParameters, dwrfEncryptor, orcEncoding);
        this.presentStream = new PresentOutputStream(compressionParameters, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, compressionParameters, dwrfEncryptor);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.<ColumnWriter>builder()
                .add(elementWriter)
                .addAll(elementWriter.getNestedColumnWriters())
                .build();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(column, columnEncoding);
        encodings.putAll(elementWriter.getColumnEncodings());
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        lengthStream.recordCheckpoint();
        presentStream.recordCheckpoint();

        elementWriter.beginRowGroup();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarArray columnarArray = toColumnarArray(block);
        writeColumnarArray(columnarArray);
    }

    private void writeColumnarArray(ColumnarArray columnarArray)
    {
        // write nulls and lengths
        for (int position = 0; position < columnarArray.getPositionCount(); position++) {
            boolean present = !columnarArray.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
                lengthStream.writeLong(columnarArray.getLength(position));
            }
        }

        // write element values
        Block elementsBlock = columnarArray.getElementsBlock();
        if (elementsBlock.getPositionCount() > 0) {
            elementWriter.writeBlock(elementsBlock);
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);

        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, 0, null, null, null, null, null, null, null, null);
        rowGroupColumnStatistics.add(statistics);
        columnStatisticsRetainedSizeInBytes += statistics.getRetainedSizeInBytes();
        nonNullValueCount = 0;

        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(column, statistics);
        columnStatistics.putAll(elementWriter.finishRowGroup());
        return columnStatistics.build();
    }

    @Override
    public void close()
    {
        closed = true;
        elementWriter.close();
        lengthStream.close();
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        columnStatistics.putAll(elementWriter.getColumnStripeStatistics());
        return columnStatistics.build();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams()
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> lengthCheckpoints = lengthStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint lengthCheckpoint = lengthCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createArrayColumnPositionList(compressed, lengthCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, StreamKind.ROW_INDEX, slice.length(), false);

        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));
        indexStreams.addAll(elementWriter.getIndexStreams());
        return indexStreams.build();
    }

    private static List<Integer> createArrayColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint lengthCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(lengthCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column).ifPresent(outputDataStreams::add);
        outputDataStreams.add(lengthStream.getStreamDataOutput(column));
        outputDataStreams.addAll(elementWriter.getDataStreams());
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return lengthStream.getBufferedBytes() + presentStream.getBufferedBytes() + elementWriter.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + lengthStream.getRetainedBytes() + presentStream.getRetainedBytes() + elementWriter.getRetainedBytes() + columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        lengthStream.reset();
        presentStream.reset();
        elementWriter.reset();
        rowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        nonNullValueCount = 0;
    }
}
