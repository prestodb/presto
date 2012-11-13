package com.facebook.presto.block.rle;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.tuple.Tuples.NULL_STRING_TUPLE;
import static org.testng.Assert.assertFalse;

public class TestRunLengthEncodedBlockCursorWithNull
        extends AbstractTestBlockCursor
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
        return new RunLengthEncodedBlock(NULL_STRING_TUPLE, 11).cursor();
    }

    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
