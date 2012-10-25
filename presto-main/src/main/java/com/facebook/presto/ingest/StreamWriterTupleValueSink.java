package com.facebook.presto.ingest;

import com.facebook.presto.block.TupleStreamPosition;
import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.block.TupleStreamWriter;
import com.facebook.presto.operator.tap.TupleValueSink;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.OutputSupplier;

import java.io.IOException;
import java.io.OutputStream;

public class StreamWriterTupleValueSink
    implements TupleValueSink
{
    private final OutputSupplier<? extends OutputStream> outputSupplier;
    private final TupleStreamSerializer tupleStreamSerializer;
    private OutputStream outputStream;
    private TupleStreamWriter tupleStreamWriter;
    private boolean finished;

    public StreamWriterTupleValueSink(OutputSupplier<? extends OutputStream> outputSupplier, TupleStreamSerializer tupleStreamSerializer)
    {
        this.outputSupplier = Preconditions.checkNotNull(outputSupplier, "outputSupplier is null");
        this.tupleStreamSerializer = Preconditions.checkNotNull(tupleStreamSerializer, "tupleStreamSerializer is null");
    }

    @Override
    public void process(TupleStreamPosition tupleStreamPosition)
    {
        Preconditions.checkNotNull(tupleStreamPosition, "tupleStreamPosition is null");
        Preconditions.checkState(!finished, "already finished");
        if (outputStream == null) {
            try {
                outputStream = outputSupplier.getOutput();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            tupleStreamWriter = tupleStreamSerializer.createTupleStreamWriter(new OutputStreamSliceOutput(outputStream));
        }
        tupleStreamWriter.append(tupleStreamPosition.asBoundedTupleStream());
    }

    @Override
    public void finished()
    {
        Preconditions.checkState(!finished, "already finished");
        finished = true;
        if (outputStream != null) {
            tupleStreamWriter.finish();
            try {
                outputStream.close();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
