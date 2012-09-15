package com.facebook.presto.block.rle;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedTupleStream;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.SliceOutput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestRunLengthEncodedSerde
{
    private SliceOutput sliceOutput;
    private RunLengthEncodedSerde rleSerde;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        sliceOutput = new DynamicSliceOutput(1024);
        rleSerde = new RunLengthEncodedSerde();
    }

    @Test
    public void testSanity() throws Exception
    {
        TupleStream tupleStream = Blocks.createBlockStream(0, "a", "b", "b", "cde", "fuu", "a", "fuu");
        rleSerde.serialize(tupleStream, sliceOutput);
        Blocks.assertBlockStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testAllSame() throws Exception
    {
        TupleStream tupleStream = Blocks.createBlockStream(0, "a", "a", "a", "a", "a", "a", "a");
        rleSerde.serialize(tupleStream, sliceOutput);
        Blocks.assertBlockStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testAllUnique() throws Exception
    {
        TupleStream tupleStream = Blocks.createBlockStream(0, "a", "b", "c", "d", "e", "f", "g");
        rleSerde.serialize(tupleStream, sliceOutput);
        Blocks.assertBlockStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testPositionGaps() throws Exception
    {
        TupleStream tupleStream = new UncompressedTupleStream(
                TupleInfo.SINGLE_VARBINARY,
                Blocks.createBlock(1, "a", "a", "b", "a", "c"),
                Blocks.createBlock(6, "c", "a", "b", "b", "b"),
                Blocks.createBlock(100, "y", "y", "a", "y", "b"),
                Blocks.createBlock(200, "b")
        );
        rleSerde.serialize(tupleStream, sliceOutput);
        Blocks.assertBlockStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testTupleWriter()
            throws Exception
    {
        UncompressedBlock block1 = Blocks.createBlock(1, "a", "a", "b", "a", "c");
        UncompressedBlock block2 = Blocks.createBlock(6, "c", "a", "b", "b", "b");
        UncompressedBlock block3 = Blocks.createBlock(100, "y", "y", "a", "y", "b");
        UncompressedBlock block4 = Blocks.createBlock(200, "b");
        TupleStream tupleStream = new UncompressedTupleStream(
                TupleInfo.SINGLE_VARBINARY,
                block1,
                block2,
                block3,
                block4
        );
        rleSerde.createTupleStreamWriter(sliceOutput)
                .append(block1)
                .append(block2)
                .append(block3)
                .append(block4)
                .finished();
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }
}
