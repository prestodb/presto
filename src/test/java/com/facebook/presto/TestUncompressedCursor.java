package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class TestUncompressedCursor extends AbstractTestUncompressedSliceCursor
{
    @Test
    public void testFirstValue()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();
        CursorAssertions.assertNextValue(cursor, 0, "apple");
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
    }

    @Test
    public void testConstructorNulls()
    {
        try {
            new UncompressedCursor(createTupleInfo(), null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException expected) {
        }
        try {
            new UncompressedCursor(null, ImmutableList.of(Blocks.createBlock(0, "a")).iterator());
            fail("Expected NullPointerException");
        }
        catch (NullPointerException expected) {
        }
    }

    @Override
    protected UncompressedCursor createCursor()
    {
        return new UncompressedCursor(createTupleInfo(), createBlocks().iterator());
    }
}
