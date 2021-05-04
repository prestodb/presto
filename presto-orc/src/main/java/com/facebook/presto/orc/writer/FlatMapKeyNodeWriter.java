package com.facebook.presto.orc.writer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FlatMapKeyNodeWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FlatMapKeyNodeWriter.class).instanceSize();

    private final int column;
    private final int sequence;
    private final ColumnWriter valueWriter;
    private final BooleanOutputStream inMapStream;
    private final CompressedMetadataWriter metadataWriter;
    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private boolean closed;
    private int nonNullValueCount;

    public int getSequence()
    {
        return this.sequence;
    }

    public FlatMapKeyNodeWriter(int column, int sequence, ColumnWriterOptions columnWriterOptions, Optional<DwrfDataEncryptor> dwrfEncryptor, MetadataWriter metadataWriter, ColumnWriter valueWriter)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(sequence >= 1, "sequence is non positive");
        this.column = column;
        this.sequence = sequence;
        this.valueWriter = valueWriter;
        inMapStream = new BooleanOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
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
            resultEncoding.put(this.sequence, encoding.getValue());
        }
        return resultEncoding.build();
    }

    @Override
    public void beginRowGroup()
    {
        valueWriter.beginRowGroup();
        inMapStream.recordCheckpoint();
    }

    public void addNulls(int count)
    {
        this.inMapStream.writeBooleans(count, false);
    }

    @Override
    public void writeBlock(Block block)
    {
        this.nonNullValueCount++;
        this.inMapStream.writeBoolean(true);
        valueWriter.writeBlock(block);
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        return valueWriter.finishRowGroup();
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

    @Override
    public List<StreamDataOutput> getIndexStreams() throws IOException
    {
        checkState(closed);
        /* Each Sequence has one ROW INDEX associated with the writer */
        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.addAll(valueWriter.getIndexStreams());
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
        StreamDataOutput inMapStreamDataOutput = inMapStream.getStreamDataOutput(column);
        // rewrite the DATA stream created by the boolean output stream to a IN_MAP stream
        Stream stream = new Stream(column, Stream.StreamKind.IN_MAP,
                toIntExact(inMapStreamDataOutput.size()),
                inMapStreamDataOutput.getStream().isUseVInts(),
                sequence, Optional.empty());
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

    public void reset() {
        closed = false;
        nonNullValueCount = 0;
        valueWriter.reset();
        inMapStream.reset();
        rowGroupColumnStatistics.clear();
    }
}
