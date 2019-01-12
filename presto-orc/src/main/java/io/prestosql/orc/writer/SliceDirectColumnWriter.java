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
package io.prestosql.orc.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.orc.OrcEncoding;
import io.prestosql.orc.checkpoint.BooleanStreamCheckpoint;
import io.prestosql.orc.checkpoint.ByteArrayStreamCheckpoint;
import io.prestosql.orc.checkpoint.LongStreamCheckpoint;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.CompressedMetadataWriter;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.RowGroupIndex;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.orc.metadata.Stream.StreamKind;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.SliceColumnStatisticsBuilder;
import io.prestosql.orc.stream.ByteArrayOutputStream;
import io.prestosql.orc.stream.LongOutputStream;
import io.prestosql.orc.stream.PresentOutputStream;
import io.prestosql.orc.stream.StreamDataOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.OrcEncoding.DWRF;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.orc.stream.LongOutputStream.createLengthOutputStream;
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

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private final Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier;
    private SliceColumnStatisticsBuilder statisticsBuilder;

    private boolean closed;

    public SliceDirectColumnWriter(int column, Type type, CompressionKind compression, int bufferSize, OrcEncoding orcEncoding, Supplier<SliceColumnStatisticsBuilder> statisticsBuilderSupplier)
    {
        checkArgument(column >= 0, "column is negative");
        this.column = column;
        this.type = requireNonNull(type, "type is null");
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DIRECT : DIRECT_V2, 0);
        this.lengthStream = createLengthOutputStream(compression, bufferSize, orcEncoding);
        this.dataStream = new ByteArrayOutputStream(compression, bufferSize);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
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
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
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
        long retainedBytes = INSTANCE_SIZE + lengthStream.getRetainedBytes() + dataStream.getRetainedBytes() + presentStream.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
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
        statisticsBuilder = statisticsBuilderSupplier.get();
    }
}
