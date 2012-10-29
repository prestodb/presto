package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSelfIdTupleStreamSerde
{
    private DynamicSliceOutput sliceOutput;
    private TupleStream tupleStream;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        sliceOutput = new DynamicSliceOutput(1024);
        tupleStream = Blocks.createBlock(0, "a", "b", "c");
    }

    @Test
    public void testRaw() throws Exception
    {
        TupleStreamSerde serde = new SelfDescriptiveSerde(new UncompressedSerde());
        serde.createSerializer()
                .createTupleStreamWriter(sliceOutput)
                .append(tupleStream)
                .finish();
        Blocks.assertTupleStreamEquals(
                // Should be able to use generic deserializer
                SelfDescriptiveSerde.DESERIALIZER.deserialize(Range.ALL, sliceOutput.slice()),
                tupleStream
        );
    }

    @Test
    public void testRle() throws Exception
    {
        TupleStreamSerde serde = new SelfDescriptiveSerde(new RunLengthEncodedSerde());
        serde.createSerializer()
                .createTupleStreamWriter(sliceOutput)
                .append(tupleStream)
                .finish();
        Blocks.assertTupleStreamEquals(
                // Should be able to use generic deserializer
                SelfDescriptiveSerde.DESERIALIZER.deserialize(Range.ALL, sliceOutput.slice()),
                tupleStream
        );
    }

    @Test
    public void testDicDicRle() throws Exception
    {
        TupleStreamSerde serde = new SelfDescriptiveSerde(new DictionarySerde(new DictionarySerde(new RunLengthEncodedSerde())));
        serde.createSerializer()
                .createTupleStreamWriter(sliceOutput)
                .append(tupleStream)
                .finish();
        Blocks.assertTupleStreamEquals(
                // Should be able to use generic deserializer
                SelfDescriptiveSerde.DESERIALIZER.deserialize(Range.ALL, sliceOutput.slice()),
                tupleStream
        );
    }
}
