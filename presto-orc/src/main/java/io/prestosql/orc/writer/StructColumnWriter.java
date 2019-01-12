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
import io.prestosql.orc.checkpoint.BooleanStreamCheckpoint;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.CompressedMetadataWriter;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.RowGroupIndex;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.orc.metadata.Stream.StreamKind;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.stream.PresentOutputStream;
import io.prestosql.orc.stream.StreamDataOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ColumnarRow;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

public class StructColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StructColumnWriter.class).instanceSize();
    private static final ColumnEncoding COLUMN_ENCODING = new ColumnEncoding(DIRECT, 0);

    private final int column;
    private final boolean compressed;
    private final PresentOutputStream presentStream;
    private final List<ColumnWriter> structFields;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private int nonNullValueCount;

    private boolean closed;

    public StructColumnWriter(int column, CompressionKind compression, int bufferSize, List<ColumnWriter> structFields)
    {
        checkArgument(column >= 0, "column is negative");
        this.column = column;
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.structFields = ImmutableList.copyOf(requireNonNull(structFields, "structFields is null"));
        this.presentStream = new PresentOutputStream(compression, bufferSize);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        ImmutableList.Builder<ColumnWriter> nestedColumnWriters = ImmutableList.builder();
        for (ColumnWriter structField : structFields) {
            nestedColumnWriters
                    .add(structField)
                    .addAll(structField.getNestedColumnWriters());
        }
        return nestedColumnWriters.build();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(column, COLUMN_ENCODING);
        structFields.stream()
                .map(ColumnWriter::getColumnEncodings)
                .forEach(encodings::putAll);
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();

        structFields.forEach(ColumnWriter::beginRowGroup);
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarRow columnarRow = toColumnarRow(block);
        writeColumnarRow(columnarRow);
    }

    private void writeColumnarRow(ColumnarRow columnarRow)
    {
        // record nulls
        for (int position = 0; position < columnarRow.getPositionCount(); position++) {
            boolean present = !columnarRow.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
            }
        }

        // write field values
        for (int i = 0; i < structFields.size(); i++) {
            ColumnWriter columnWriter = structFields.get(i);
            Block fieldBlock = columnarRow.getField(i);
            if (fieldBlock.getPositionCount() > 0) {
                columnWriter.writeBlock(fieldBlock);
            }
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, 0, null, null, null, null, null, null, null, null);
        rowGroupColumnStatistics.add(statistics);
        nonNullValueCount = 0;

        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(column, statistics);
        structFields.stream()
                .map(ColumnWriter::finishRowGroup)
                .forEach(columnStatistics::putAll);
        return columnStatistics.build();
    }

    @Override
    public void close()
    {
        closed = true;
        structFields.forEach(ColumnWriter::close);
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.put(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        structFields.stream()
                .map(ColumnWriter::getColumnStripeStatistics)
                .forEach(columnStatistics::putAll);
        return columnStatistics.build();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createStructColumnPositionList(compressed, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, StreamKind.ROW_INDEX, slice.length(), false);

        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));
        for (ColumnWriter structField : structFields) {
            indexStreams.addAll(structField.getIndexStreams(metadataWriter));
        }
        return indexStreams.build();
    }

    private static List<Integer> createStructColumnPositionList(
            boolean compressed,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column).ifPresent(outputDataStreams::add);
        for (ColumnWriter structField : structFields) {
            outputDataStreams.addAll(structField.getDataStreams());
        }
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        long bufferedBytes = presentStream.getBufferedBytes();
        for (ColumnWriter structField : structFields) {
            bufferedBytes += structField.getBufferedBytes();
        }
        return bufferedBytes;
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + presentStream.getRetainedBytes();
        for (ColumnWriter structField : structFields) {
            retainedBytes += structField.getRetainedBytes();
        }
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        presentStream.reset();
        structFields.forEach(ColumnWriter::reset);
        rowGroupColumnStatistics.clear();
        nonNullValueCount = 0;
    }
}
