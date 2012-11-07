package com.facebook.presto.block.rle;

import com.facebook.presto.util.Range;
import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static org.testng.Assert.assertFalse;

public class TestRunLengthEncodedBlockCursor extends AbstractTestBlockCursor
{
    @Test
    public void testFirstValue()
    {
        BlockCursor cursor = createTestCursor();
        assertFalse(cursor.advanceNextValue());
    }

    @Override
    @Test(enabled=false)
    public void testAdvanceNextValue()
    {
    }

    @Override
    @Test(enabled=false)
    public void testMixedValueAndPosition()
    {
    }

    @Override
    @Test(enabled=false)
    public void testNextValuePosition()
    {
    }

    @Override
    protected RunLengthEncodedBlockCursor createTestCursor()
    {
        return new RunLengthEncodedBlock(createTuple("cherry"), Range.create(0, 10)).cursor();
    }

    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(0,
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
