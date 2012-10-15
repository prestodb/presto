package com.facebook.presto.ingest;

import com.facebook.presto.block.*;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.slice.Slices;
import com.google.common.io.OutputSupplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class TestStreamWriterTupleValueSink
{
    private StreamWriterTupleValueSink streamWriterTupleValueSink;
    private ByteArrayOutputStream byteArrayOutputStream;
    private TupleStreamSerde serde;
    private TupleStream tupleStream;
    private Cursor cursor;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        byteArrayOutputStream = new ByteArrayOutputStream(1024);
        serde = TupleStreamSerdes.Encoding.RAW.createSerde();
        streamWriterTupleValueSink = new StreamWriterTupleValueSink(
                new OutputSupplier<OutputStream>() {
                    @Override
                    public OutputStream getOutput() throws IOException
                    {
                        return byteArrayOutputStream;
                    }
                },
                serde.createSerializer()
        );
        tupleStream = new GroupByOperator(Blocks.createTupleStream(0, "a", "a", "b", "b"));
        cursor = tupleStream.cursor(new QuerySession());
    }

    @Test
    public void testSanity() throws Exception
    {
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
