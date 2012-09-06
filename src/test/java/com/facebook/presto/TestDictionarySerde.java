package com.facebook.presto;

import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.Iterables;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestDictionarySerde {
    private SliceOutput sliceOutput;
    private DictionarySerde dictionarySerde;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        sliceOutput = new DynamicSliceOutput(1024);
        dictionarySerde = new DictionarySerde(new UncompressedBlockSerde());
    }

    @Test
    public void testSanity() throws Exception {
        BlockStream blockStream = Blocks.createBlockStream(0, "a", "b", "cde", "fuu", "a", "fuu");
        dictionarySerde.serialize(blockStream, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        Iterables.concat(blockStream),
                        Iterables.concat(dictionarySerde.deserialize(sliceOutput.slice()))
                )
        );
    }

    @Test
    public void testAllSame() throws Exception {
        BlockStream blockStream = Blocks.createBlockStream(0, "a", "a", "a", "a", "a", "a", "a");
        dictionarySerde.serialize(blockStream, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        Iterables.concat(blockStream),
                        Iterables.concat(dictionarySerde.deserialize(sliceOutput.slice()))
                )
        );
    }

    @Test
    public void testAllUnique() throws Exception {
        BlockStream blockStream = Blocks.createBlockStream(0, "a", "b", "c", "d", "e", "f", "g");
        dictionarySerde.serialize(blockStream, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        Iterables.concat(blockStream),
                        Iterables.concat(dictionarySerde.deserialize(sliceOutput.slice()))
                )
        );
    }
}
