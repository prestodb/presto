package com.facebook.presto.block.rle;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class TestRunLengthEncodedBlockCursor extends AbstractTestBlockCursor
{

    @Override
    protected RunLengthEncodedBlockCursor createTestCursor()
    {
        return new RunLengthEncodedBlock(createTuple("cherry"), 11).cursor();
    }

    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry",
                "cherry");
    }
}
