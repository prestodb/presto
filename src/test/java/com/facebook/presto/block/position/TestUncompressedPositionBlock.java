package com.facebook.presto.block.position;

import com.facebook.presto.block.Cursor;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;

import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUncompressedPositionBlock
{
    @Test
    public void testBasic()
            throws Exception
    {
        UncompressedPositionBlock block = new UncompressedPositionBlock(0, 1, 2, 3, 4, 10, 11, 12, 13, 14);

        Cursor cursor = block.cursor();

        assertNextPosition(cursor, 0);
        assertNextPosition(cursor, 1);
        assertNextPosition(cursor, 2);
        assertNextPosition(cursor, 3);
        assertNextPosition(cursor, 4);

        assertNextPosition(cursor, 10);
        assertNextPosition(cursor, 11);
        assertNextPosition(cursor, 12);
        assertNextPosition(cursor, 13);
        assertNextPosition(cursor, 14);

        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        UncompressedPositionBlock block = new UncompressedPositionBlock(0, 1, 2, 3, 4, 10, 11, 12, 13, 14);

        Cursor cursor = block.cursor();

        // advance to beginning
        assertTrue(cursor.advanceToPosition(0));
        assertEquals(cursor.getPosition(), 0);

        // advance to gap
        assertTrue(cursor.advanceToPosition(7));
        assertEquals(cursor.getPosition(), 10);

        // advance to other valid position
        assertTrue(cursor.advanceToPosition(12));
        assertEquals(cursor.getPosition(), 12);

        // advance past end
        assertFalse(cursor.advanceToPosition(20));
        assertTrue(cursor.isFinished());

        try {
            cursor.getPosition();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }

        assertTrue(cursor.isFinished());
    }
}
