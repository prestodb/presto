package com.facebook.presto;

import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.SliceOutput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.Blocks.assertBlockStreamEquals;

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
        BlockStream<?> blockStream = Blocks.createBlockStream(0, "a", "b", "cde", "fuu", "a", "fuu");
        dictionarySerde.serialize(blockStream, sliceOutput);
        assertBlockStreamEquals(dictionarySerde.deserialize(sliceOutput.slice()), blockStream);
    }

    @Test
    public void testAllSame() throws Exception {
        BlockStream<?> blockStream = Blocks.createBlockStream(0, "a", "a", "a", "a", "a", "a", "a");
        dictionarySerde.serialize(blockStream, sliceOutput);
        BlockStream<DictionaryEncodedBlock> deserialize = dictionarySerde.deserialize(sliceOutput.slice());
        assertBlockStreamEquals(deserialize, blockStream);
    }

    @Test
    public void testAllUnique() throws Exception {
        BlockStream<?> blockStream = Blocks.createBlockStream(0, "a", "b", "c", "d", "e", "f", "g");
        dictionarySerde.serialize(blockStream, sliceOutput);
        assertBlockStreamEquals(dictionarySerde.deserialize(sliceOutput.slice()), blockStream);
    }
}
