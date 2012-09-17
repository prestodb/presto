package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.Tuples;
import com.facebook.presto.block.AbstractTestCursor;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.CursorAssertions;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.facebook.presto.block.uncompressed.UncompressedTupleStream;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.Blocks.createBlock;
import static com.facebook.presto.block.CursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestRunLengthEncodedCursor extends AbstractTestCursor
{
    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        CursorAssertions.assertNextValue(cursor, 0, "apple");
        CursorAssertions.assertNextValue(cursor, 5, "banana");
        CursorAssertions.assertNextValue(cursor, 20, "cherry");
        CursorAssertions.assertNextValue(cursor, 30, "date");

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceToPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        // advance to first position
        assertTrue(cursor.advanceToPosition(0));
        assertCurrentValue(cursor, 0, "apple");

        // skip to position in first block
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, "apple");

        // advance to same position
        assertTrue(cursor.advanceToPosition(2));
        assertCurrentValue(cursor, 2, "apple");

        // skip to position in same block
        assertTrue(cursor.advanceToPosition(4));
        assertCurrentValue(cursor, 4, "apple");

        // skip to position in middle block
        assertTrue(cursor.advanceToPosition(21));
        assertCurrentValue(cursor, 21, "cherry");

        // skip to position in gap
        assertTrue(cursor.advanceToPosition(25));
        assertCurrentValue(cursor, 30, "date");

        // skip backwards
        try {
            cursor.advanceToPosition(20);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 30, "date");
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));

        assertTrue(cursor.isFinished());
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
        assertTrue(cursor.advanceToPosition(4));

        // force jump to next block
        CursorAssertions.assertNextPosition(cursor, 5, "banana");
    }

    @Test
    public void testAdvanceNextValueAtEndOfBlock()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        // first, advance to end of first block
        assertTrue(cursor.advanceToPosition(4));

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

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceNextPositionThrows()
    {
        RunLengthEncodedCursor cursor = createCursor();

        // first, skip to end
        while (cursor.advanceNextPosition()) {
        }

        // advance past end
        assertFalse(cursor.advanceNextPosition());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testAdvanceNextValueThrows()
    {
        RunLengthEncodedCursor cursor = createCursor();

        // first, skip to end
        while (cursor.advanceNextValue()) {
        }

        // advance past end
        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testGetCurrentValueEndPosition()
            throws Exception
    {
        RunLengthEncodedCursor cursor = createCursor();

        cursor.advanceNextValue();
        assertEquals(cursor.getCurrentValueEndPosition(), 4);

        cursor.advanceNextValue();
        assertEquals(cursor.getCurrentValueEndPosition(), 7);

        cursor.advanceNextValue();
        assertEquals(cursor.getCurrentValueEndPosition(), 21);

        cursor.advanceNextValue();
        assertEquals(cursor.getCurrentValueEndPosition(), 30);
    }

    @Test
    public void testTupleInfo()
            throws Exception
    {
        Cursor cursor = createCursor();
        assertEquals(cursor.getTupleInfo(), TupleInfo.SINGLE_VARBINARY);

        try {
            new UncompressedCursor(TupleInfo.SINGLE_VARBINARY, null);
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
    public void testMixedValueAndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextValue(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextValue(cursor, 20, "cherry");
        assertNextPosition(cursor, 21, "cherry");
        assertNextValue(cursor, 30, "date");

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Override
    protected TupleStream createExpectedValues()
    {
        return new UncompressedTupleStream(TupleInfo.SINGLE_VARBINARY, ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "apple", "apple"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "cherry", "cherry"),
                createBlock(30, "date")));
    }


    @Override
    protected RunLengthEncodedCursor createCursor()
    {
        List<RunLengthEncodedBlock> blocks = ImmutableList.of(
                new RunLengthEncodedBlock(Tuples.createTuple("apple"), Range.create(0, 4)),
                new RunLengthEncodedBlock(Tuples.createTuple("banana"), Range.create(5, 7)),
                new RunLengthEncodedBlock(Tuples.createTuple("cherry"), Range.create(20, 21)),
                new RunLengthEncodedBlock(Tuples.createTuple("date"), Range.create(30, 30)));

        return new RunLengthEncodedCursor(TupleInfo.SINGLE_VARBINARY, blocks.iterator());
    }
}
