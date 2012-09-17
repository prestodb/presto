package com.facebook.presto.block.rle;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedTupleStream;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.base.Charsets.UTF_8;

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
        TupleStream tupleStream = Blocks.createTupleStream(0, "a", "b", "b", "cde", "fuu", "a", "fuu");
        TupleStreamSerdes.serialize(rleSerde, tupleStream, sliceOutput);
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testAllSame() throws Exception
    {
        TupleStream tupleStream = Blocks.createTupleStream(0, "a", "a", "a", "a", "a", "a", "a");
        TupleStreamSerdes.serialize(rleSerde, tupleStream, sliceOutput);
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testAllUnique() throws Exception
    {
        TupleStream tupleStream = Blocks.createTupleStream(0, "a", "b", "c", "d", "e", "f", "g");
        TupleStreamSerdes.serialize(rleSerde, tupleStream, sliceOutput);
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
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
        TupleStreamSerdes.serialize(rleSerde, tupleStream, sliceOutput);
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(TupleInfo.Type.FIXED_INT_64, TupleInfo.Type.VARIABLE_BINARY);
        TupleStream tupleStream = new UncompressedTupleStream(
                new TupleInfo(TupleInfo.Type.FIXED_INT_64, TupleInfo.Type.VARIABLE_BINARY),
                new BlockBuilder(0, tupleInfo)
                        .append(0L).append(Slices.wrappedBuffer("a".getBytes(UTF_8)))
                        .append(5L).append(Slices.wrappedBuffer("b".getBytes(UTF_8)))
                        .append(5L).append(Slices.wrappedBuffer("b".getBytes(UTF_8)))
                        .append(5L).append(Slices.wrappedBuffer("b".getBytes(UTF_8)))
                        .append(-1L).append(Slices.wrappedBuffer("ccc".getBytes(UTF_8)))
                        .append(-1L).append(Slices.wrappedBuffer("ccc".getBytes(UTF_8)))
                        .build()
        );
        TupleStreamSerdes.serialize(rleSerde, tupleStream, sliceOutput);
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
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
                .close();
        Blocks.assertTupleStreamEquals(tupleStream, rleSerde.deserialize(sliceOutput.slice()));
    }
}
