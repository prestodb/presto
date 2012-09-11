package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class TestUncompressedSliceCursor extends AbstractTestUncompressedSliceCursor
{
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

    @Test
    public void testGetLong()
            throws Exception
    {
        Cursor cursor = createCursor();

        try {
            cursor.getLong(0);
            fail("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException expected) {
        }
    }

    @Override
    protected Cursor createCursor()
    {
        return new UncompressedSliceCursor(createBlocks().iterator());
    }
}
