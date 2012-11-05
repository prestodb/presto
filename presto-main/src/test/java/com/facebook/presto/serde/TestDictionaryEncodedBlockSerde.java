package com.facebook.presto.serde;

import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.nblock.BlockAssertions.assertBlocksEquals;
import static com.facebook.presto.nblock.BlockIterables.createBlockIterable;
import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;

public class TestDictionaryEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        DictionaryEncodedBlocksSerde blocksSerde = new DictionaryEncodedBlocksSerde(new SimpleBlocksSerde(UNCOMPRESSED_BLOCK_SERDE));

        UncompressedBlock expectedBlock = new BlockBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blocksSerde.writeBlocks(sliceOutput, expectedBlock, expectedBlock, expectedBlock);
        BlockIterable actualBlocks = blocksSerde.createBlocksReader(sliceOutput.slice(), 0);
        assertBlocksEquals(actualBlocks, createBlockIterable(new BlockBuilder(0, SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build()));
    }
}
