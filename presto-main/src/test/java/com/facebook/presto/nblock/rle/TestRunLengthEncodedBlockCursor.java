package com.facebook.presto.nblock.rle;

import com.facebook.presto.Range;
import com.facebook.presto.nblock.AbstractTestBlockCursor;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockCursor;
import org.testng.annotations.Test;

import static com.facebook.presto.Tuples.createTuple;
import static com.facebook.presto.nblock.BlockAssertions.createStringsBlock;
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
    public void testAdvanceNextValue()
    {
    }

    @Override
    public void testMixedValueAndPosition()
    {
    }

    @Override
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
