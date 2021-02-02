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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.ByteArrayStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.SliceColumnStatisticsBuilder;
import com.facebook.presto.orc.stream.ByteArrayOutputStream;
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
import java.util.function.Supplier;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SliceDirectColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDirectColumnWriter.class).instanceSize();
    private final int column;
    private final Type type;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream lengthStream;
    private final ByteArrayOutputStream dataStream;
    private final PresentOutputStream presentStream;
    private final CompressedMetadataWriter metadataWriter;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;

    private final Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier;
    private SliceColumnStatisticsBuilder statisticsBuilder;

    private boolean closed;

    public SliceDirectColumnWriter(
            int column,
            Type type,
            CompressionParameters compressionParameters,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier,
            MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        requireNonNull(compressionParameters, "compressionParameters is null");
        requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        requireNonNull(metadataWriter, "metadataWriter is null");
        this.column = column;
        this.type = requireNonNull(type, "type is null");
        this.compressed = compressionParameters.getKind() != NONE;
        this.columnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DIRECT : DIRECT_V2, 0);
        this.lengthStream = createLengthOutputStream(compressionParameters, dwrfEncryptor, orcEncoding);
        this.dataStream = new ByteArrayOutputStream(compressionParameters, dwrfEncryptor);
        this.presentStream = new PresentOutputStream(compressionParameters, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, compressionParameters, dwrfEncryptor);
        this.statisticsBuilderSupplier = statisticsBuilderSupplier;
        statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(column, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        checkState(!closed);
        presentStream.recordCheckpoint();
        lengthStream.recordCheckpoint();
        dataStream.recordCheckpoint();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                Slice value = type.getSlice(block, position);
                lengthStream.writeLong(value.length());
                dataStream.writeSlice(value);
                statisticsBuilder.addValue(value);
            }
        }
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
        checkState(!closed);
        closed = true;
        lengthStream.close();
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
    public List<StreamDataOutput> getIndexStreams()
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> lengthCheckpoints = lengthStream.getCheckpoints();
        List<ByteArrayStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint lengthCheckpoint = lengthCheckpoints.get(groupId);
            ByteArrayStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(compressed, lengthCheckpoint, dataCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createSliceColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint lengthCheckpoint,
            ByteArrayStreamCheckpoint dataCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
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
        outputDataStreams.add(dataStream.getStreamDataOutput(column));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return lengthStream.getBufferedBytes() + dataStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + lengthStream.getRetainedBytes() + dataStream.getRetainedBytes() + presentStream.getRetainedBytes() + columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        lengthStream.reset();
        dataStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();
    }
}
