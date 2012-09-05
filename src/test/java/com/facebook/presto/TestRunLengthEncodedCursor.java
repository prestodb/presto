package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static org.testng.Assert.assertFalse;

public class TestRunLengthEncodedCursor
{
    @Test
    public void testFirstValue()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();
        CursorAssertions.assertNextValue(cursor, 0, "apple");
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
    }


    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        CursorAssertions.assertNextValue(cursor, 0, "apple");
        CursorAssertions.assertNextValue(cursor, 5, "banana");
        CursorAssertions.assertNextValue(cursor, 20, "cherry");
        CursorAssertions.assertNextValue(cursor, 30, "date");

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        RunLengthEncodedCursor cursor = createCursor();

        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "apple");
        CursorAssertions.assertNextPosition(cursor, 4, "apple");
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
        CursorAssertions.assertNextPosition(cursor, 6, "banana");
        CursorAssertions.assertNextPosition(cursor, 7, "banana");
        CursorAssertions.assertNextPosition(cursor, 20, "cherry");
        CursorAssertions.assertNextPosition(cursor, 21, "cherry");
        CursorAssertions.assertNextPosition(cursor, 30, "date");

        assertFalse(cursor.hasNextPosition());
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        // first, skip to middle of a block
        CursorAssertions.assertNextValue(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 5, "banana");
    }

    @Test
    public void testAdvanceToNextPositionAdvancesValue()
    {
        RunLengthEncodedCursor cursor = createCursor();

        // first, advance to end of first block
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "apple");
        CursorAssertions.assertNextPosition(cursor, 4, "apple");

        // force jump to next block
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        // first, advance to end of first block
        CursorAssertions.assertNextPosition(cursor, 0, "apple");
        CursorAssertions.assertNextPosition(cursor, 1, "apple");
        CursorAssertions.assertNextPosition(cursor, 2, "apple");
        CursorAssertions.assertNextPosition(cursor, 3, "apple");
        CursorAssertions.assertNextPosition(cursor, 4, "apple");

        // force jump to next block
        CursorAssertions.assertNextValue(cursor, 5, "banana");
    }

    @Test
    public void testNextValuePosition()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        CursorAssertions.assertNextValuePosition(cursor, 0);
        CursorAssertions.assertNextValuePosition(cursor, 5);
        CursorAssertions.assertNextValuePosition(cursor, 20);
        CursorAssertions.assertNextValuePosition(cursor, 30);

        assertFalse(cursor.hasNextValue());
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testAdvanceNextPositionThrows()
    {
        RunLengthEncodedCursor cursor = createCursor();

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
        RunLengthEncodedCursor cursor = createCursor();

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
        RunLengthEncodedCursor cursor = createCursor();

        // first, skip to end
        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
        }

        // peek past end
        cursor.peekNextValuePosition();
    }

    private RunLengthEncodedCursor createCursor()
    {
        TupleInfo info = new TupleInfo(TupleInfo.Type.VARIABLE_BINARY);

        List<RunLengthEncodedBlock> blocks = ImmutableList.of(
                new RunLengthEncodedBlock(Tuples.createTuple("apple"), Range.create(0, 4)),
                new RunLengthEncodedBlock(Tuples.createTuple("banana"), Range.create(5, 7)),
                new RunLengthEncodedBlock(Tuples.createTuple("cherry"), Range.create(20, 21)),
                new RunLengthEncodedBlock(Tuples.createTuple("date"), Range.create(30, 30)));

        return new RunLengthEncodedCursor(info, blocks.iterator());
    }

}
