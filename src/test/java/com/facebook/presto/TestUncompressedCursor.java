package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.CursorAssertions.assertNextPosition;
import static com.facebook.presto.CursorAssertions.assertNextValue;
import static com.facebook.presto.CursorAssertions.assertNextValuePosition;
import static org.testng.Assert.assertFalse;

public class TestUncompressedCursor
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
    public void testAdvanceNextValue()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextValue(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextValue(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextValue(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextValue(cursor, 7, "banana");
        assertNextValue(cursor, 20, "cherry");
        assertNextValue(cursor, 21, "cherry");
        assertNextValue(cursor, 30, "date");

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        UncompressedCursor cursor = createCursor();

        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextPosition(cursor, 20, "cherry");
        assertNextPosition(cursor, 21, "cherry");
        assertNextPosition(cursor, 30, "date");

        assertFalse(cursor.hasNextPosition());
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        // first, skip to middle of a block
        CursorAssertions.assertNextValue(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 2, "apple");
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        UncompressedCursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "banana");
        CursorAssertions.assertNextPosition(cursor, 4, "banana");
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
        CursorAssertions.assertNextPosition(cursor, 6, "banana");
        CursorAssertions.assertNextPosition(cursor, 7, "banana");

        // force jump to next block
        CursorAssertions.assertNextPosition(cursor, 20, "cherry");
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        // first, advance to end of a block
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "banana");
        CursorAssertions.assertNextPosition(cursor, 4, "banana");
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
        CursorAssertions.assertNextPosition(cursor, 6, "banana");
        CursorAssertions.assertNextPosition(cursor, 7, "banana");

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 20, "cherry");
    }

    @Test
    public void testNextValuePosition()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        assertNextValuePosition(cursor, 0);
        assertNextValuePosition(cursor, 1);
        assertNextValuePosition(cursor, 2);
        assertNextValuePosition(cursor, 3);
        assertNextValuePosition(cursor, 4);
        assertNextValuePosition(cursor, 5);
        assertNextValuePosition(cursor, 6);
        assertNextValuePosition(cursor, 7);
        assertNextValuePosition(cursor, 20);
        assertNextValuePosition(cursor, 21);
        assertNextValuePosition(cursor, 30);

        assertFalse(cursor.hasNextValue());
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testAdvanceNextPositionThrows()
    {
        UncompressedCursor cursor = createCursor();

        // first, skip to end
        while (cursor.hasNextPosition()) {
            cursor.advanceNextPosition();
        }

        // advance past end
        cursor.advanceNextPosition();
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testAdvanceNextValueThrows()
    {
        UncompressedCursor cursor = createCursor();

        // first, skip to end
        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
        }

        // advance past end
        cursor.advanceNextValue();
    }


    @Test(expectedExceptions = NoSuchElementException.class)
    public void testPeekNextValuePositionThrows()
    {
        UncompressedCursor cursor = createCursor();

        // first, skip to end
        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
        }

        // peek past end
        cursor.peekNextValuePosition();
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        UncompressedCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextValue(cursor, 20, "cherry");
        assertNextPosition(cursor, 21, "cherry");
        assertNextValue(cursor, 30, "date");

        assertFalse(cursor.hasNextPosition());
        assertFalse(cursor.hasNextValue());
    }

    private UncompressedCursor createCursor()
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        List<UncompressedValueBlock> blocks = ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "banana", "banana"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "cherry", "cherry"),
                createBlock(30, "date"));

        return new UncompressedCursor(info, blocks.iterator());
    }
}
