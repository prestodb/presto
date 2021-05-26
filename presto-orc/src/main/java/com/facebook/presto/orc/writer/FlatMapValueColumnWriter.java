
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
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.BooleanOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public class FlatMapValueColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FlatMapValueColumnWriter.class).instanceSize();

    private final int column;
    private final int dwrfSequence;
    private final ColumnWriter valueWriter;
    private final BooleanOutputStream inMapStream;
    private final CompressedMetadataWriter metadataWriter;

    private int lastWritePosition;
    private boolean closed;

    public int getDwrfSequence()
    {
        return this.dwrfSequence;
    }

    public FlatMapValueColumnWriter(int column, int dwrfSequence, ColumnWriterOptions columnWriterOptions, Optional<DwrfDataEncryptor> dwrfEncryptor, MetadataWriter metadataWriter, ColumnWriter valueWriter)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(dwrfSequence >= 1, "sequence is non positive");
        this.column = column;
        this.dwrfSequence = dwrfSequence;
        this.valueWriter = valueWriter;
        this.inMapStream = new BooleanOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        lastWritePosition = -1;
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return valueWriter.getNestedColumnWriters();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        Map<Integer, ColumnEncoding> valueEncoding = valueWriter.getColumnEncodings();
        ImmutableMap.Builder<Integer, ColumnEncoding> resultEncoding = ImmutableMap.builder();
        for (Map.Entry<Integer, ColumnEncoding> encoding : valueEncoding.entrySet()) {
            resultEncoding.put(this.dwrfSequence, encoding.getValue());
        }
        return resultEncoding.build();
    }

    @Override
    public void beginRowGroup()
    {
        valueWriter.beginRowGroup();
        inMapStream.recordCheckpoint();
        lastWritePosition = -1;
    }

    private int getNullCount(int lastWritePosition, int currentWritePosition)
    {
        return currentWritePosition - lastWritePosition - 1;
    }

    @Override
    public void writeBlock(Block block)
    {
        this.inMapStream.writeBoolean(true);
        valueWriter.writeBlock(block);
    }

    public void writeBlock(Block block, int rowGroupPosition)
    {
        this.inMapStream.writeBooleans(getNullCount(lastWritePosition, rowGroupPosition), false);
        lastWritePosition = rowGroupPosition;
        this.writeBlock(block);
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        return valueWriter.finishRowGroup();
    }

    public Map<Integer, ColumnStatistics> finishRowGroup(Integer rowGroupPosition)
    {
        this.inMapStream.writeBooleans(getNullCount(lastWritePosition, rowGroupPosition), false);
        this.lastWritePosition = rowGroupPosition;
        return valueWriter.finishRowGroup();
    }

    public void finishRowGroups(List<Integer> rowGroupPositions)
    {
        for (Integer rowGroupPosition : rowGroupPositions) {
            valueWriter.beginRowGroup();
            this.inMapStream.writeBooleans(rowGroupPosition, false);
            inMapStream.recordCheckpoint();
            valueWriter.finishRowGroup();
        }
    }

    @Override
    public void close()
    {
        closed = true;
        valueWriter.close();
        inMapStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        columnStatistics.putAll(valueWriter.getColumnStripeStatistics());
        return columnStatistics.build();
    }

    /* This function is a test hook to verify that we are updating the state of inmap stream */
    public int getRowGroupPosition()
    {
        return lastWritePosition;
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<BooleanStreamCheckpoint>> inMapStreamCheckpointsArg) throws IOException
    {
        checkState(closed);
        /* Each Sequence has one ROW INDEX associated with the writer */
        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.addAll(valueWriter.getIndexStreams(Optional.of(inMapStream.getCheckpoints())));
        return indexStreams.build();
    }

    /**
     * Get the data streams to be written.
     */
    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        StreamDataOutput inMapStreamDataOutput = inMapStream.getStreamDataOutput(column, dwrfSequence);
        // rewrite the DATA stream created by the boolean output stream to a IN_MAP stream
        Stream stream = new Stream(column, dwrfSequence, Stream.StreamKind.IN_MAP,
                toIntExact(inMapStreamDataOutput.size()),
                inMapStreamDataOutput.getStream().isUseVInts(),
                Optional.empty());
        outputDataStreams.add(new StreamDataOutput(
                sliceOutput -> {
                    inMapStreamDataOutput.writeData(sliceOutput);
                    return stream.getLength();
                },
                stream));
        outputDataStreams.addAll(valueWriter.getDataStreams());
        return outputDataStreams.build();
    }

    public long getBufferedBytes()
    {
        return valueWriter.getBufferedBytes() +
                inMapStream.getBufferedBytes();
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + valueWriter.getRetainedBytes() +
                inMapStream.getRetainedBytes();
    }

    public void reset()
    {
        closed = false;
        valueWriter.reset();
        inMapStream.reset();
        lastWritePosition = -1;
    }
}
