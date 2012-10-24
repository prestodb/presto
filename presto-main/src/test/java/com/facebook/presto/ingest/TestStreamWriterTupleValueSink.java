package com.facebook.presto.ingest;

import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.slice.Slices;
import com.google.common.io.OutputSupplier;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class TestStreamWriterTupleValueSink
{
    @Test
    public void testSanity() throws Exception
    {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
        TupleStreamSerde serde = TupleStreamSerdes.Encoding.RAW.createSerde();
        StreamWriterTupleValueSink streamWriterTupleValueSink = new StreamWriterTupleValueSink(
                new OutputSupplier<OutputStream>() {
                    @Override
                    public OutputStream getOutput() throws IOException
                    {
                        return byteArrayOutputStream;
                    }
                },
                serde.createSerializer()
        );
        TupleStream tupleStream = new GroupByOperator(Blocks.createTupleStream(0, "a", "a", "b", "b"));
        Cursor cursor = tupleStream.cursor(new QuerySession());
        
        cursor.advanceNextValue();
        streamWriterTupleValueSink.process(Cursors.asTupleStreamPosition(cursor));
        cursor.advanceNextValue();
        streamWriterTupleValueSink.process(Cursors.asTupleStreamPosition(cursor));
        cursor.advanceNextValue();
        streamWriterTupleValueSink.finished();

        TupleStream resultStream = serde.createDeserializer().deserialize(Slices.wrappedBuffer(byteArrayOutputStream.toByteArray()));
        Blocks.assertTupleStreamEquals(resultStream, tupleStream);
    }
}
