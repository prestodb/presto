package com.facebook.presto.block.dictionary;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.SliceOutput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.block.Blocks.assertBlockStreamEquals;

public class TestDictionarySerde
{
    private SliceOutput sliceOutput;
    private DictionarySerde dictionarySerde;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
            throws Exception
    {
        sliceOutput = new DynamicSliceOutput(1024);
        dictionarySerde = new DictionarySerde(new RunLengthEncodedSerde());
    }

    @Test
    public void testSanity()
            throws Exception
    {
        BlockStream blockStream = Blocks.createBlockStream(0, "a", "b", "cde", "fuu", "a", "fuu");
        dictionarySerde.serialize(blockStream, sliceOutput);
        assertBlockStreamEquals(dictionarySerde.deserialize(sliceOutput.slice()), blockStream);
    }

    @Test
    public void testAllSame()
            throws Exception
    {
        BlockStream blockStream = Blocks.createBlockStream(0, "a", "a", "a", "a", "a", "a", "a");
        dictionarySerde.serialize(blockStream, sliceOutput);
        BlockStream deserialize = dictionarySerde.deserialize(sliceOutput.slice());
        assertBlockStreamEquals(deserialize, blockStream);
    }

    @Test
    public void testAllUnique()
            throws Exception
    {
        BlockStream blockStream = Blocks.createBlockStream(0, "a", "b", "c", "d", "e", "f", "g");
        dictionarySerde.serialize(blockStream, sliceOutput);
        assertBlockStreamEquals(dictionarySerde.deserialize(sliceOutput.slice()), blockStream);
    }

    @Test
    public void testPositionGaps()
            throws Exception
    {
        BlockStream blockStream = new UncompressedBlockStream(
                new TupleInfo(VARIABLE_BINARY),
                Blocks.createBlock(1, "a", "a", "b", "a", "c"),
                Blocks.createBlock(6, "c", "a", "b", "b", "b"),
                Blocks.createBlock(100, "y", "y", "a", "y", "b"),
                Blocks.createBlock(200, "b")
        );
        dictionarySerde.serialize(blockStream, sliceOutput);
        Blocks.assertBlockStreamEquals(blockStream, dictionarySerde.deserialize(sliceOutput.slice()));
    }
}
