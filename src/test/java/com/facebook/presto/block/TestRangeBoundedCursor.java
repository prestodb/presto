package com.facebook.presto.block;

import com.facebook.presto.Range;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestRangeBoundedCursor
{
    @Test
    public void testFullRange() throws Exception
    {
        // Total Range: 0-7
        TupleStream originalTupleStream = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Cursor cursor = originalTupleStream.cursor();
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(0, 7)),
                originalTupleStream.cursor()
        );
        assertTrue(cursor.isFinished()); // Underlying cursor should be finished
    }

    @Test
    public void testSupersetRange() throws Exception
    {
        TupleStream originalTupleStream = Blocks.createTupleStream(5, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Cursor cursor = originalTupleStream.cursor();
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(0, 100)),
                originalTupleStream.cursor()
        );
        assertTrue(cursor.isFinished()); // Underlying cursor should be finished
    }

    @Test
    public void testBeginningRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(0, 2)),
                Blocks.createTupleStream(0, "a", "bb", "c").cursor()
        );
        assertEquals(cursor.getPosition(), 3); // Should be end+1
    }

    @Test
    public void testMidRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(3, 6)),
                Blocks.createTupleStream(3, "d", "e", "e", "a").cursor()
        );
    }

    @Test
    public void testEndRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(6, 7)),
                Blocks.createTupleStream(6, "a", "bb").cursor()
        );
    }

    @Test
    public void testPreinitAndBeforeStart() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertTrue(cursor.advanceNextPosition());
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(1, 2)),
                Blocks.createTupleStream(1, "bb", "c").cursor()
        );
        assertEquals(cursor.getPosition(), 3); // Should be end+1
    }

    @Test
    public void testPreinitAndAlreadyInRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.advanceNextPosition()); // Cursor is at position 2
        assertCursorsEqual(
                RangeBoundedCursor.bound(cursor, Range.create(1, 3)),
                Blocks.createTupleStream(2, "c", "d").cursor() // Only positions 2 & 3 should be emitted
        );
        assertEquals(cursor.getPosition(), 4); // Should be end+1
    }

    @Test
    public void testPreinitAndExceedingRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertTrue(cursor.advanceNextPosition());
        Cursor rangedCursor = RangeBoundedCursor.bound(cursor, Range.create(10, 13));
        assertFalse(rangedCursor.advanceNextPosition());
        assertTrue(rangedCursor.isFinished());
    }

    @Test
    public void testPreinitAndBeforeRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(10, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertTrue(cursor.advanceNextPosition());
        Cursor rangedCursor = RangeBoundedCursor.bound(cursor, Range.create(0, 1));
        assertFalse(rangedCursor.advanceNextPosition());
        assertTrue(rangedCursor.isFinished());
    }

    // TODO: replace with new Cursors lib that will be added
    private static void assertCursorsEqual(Cursor actual, Cursor expected)
    {
        assertFalse(actual.isValid());
        assertFalse(expected.isValid());
        while (actual.advanceNextPosition()) {
            assertTrue(expected.advanceNextPosition());
            assertTrue(actual.isValid());
            assertTrue(expected.isValid());
            assertEquals(actual.getPosition(), expected.getPosition());
            assertTrue(actual.currentTupleEquals(expected.getTuple()));
            assertEquals(actual.getCurrentValueEndPosition(), expected.getCurrentValueEndPosition());
            assertEquals(actual.getTuple(), expected.getTuple());
        }
        assertFalse(expected.advanceNextPosition());
        assertTrue(actual.isFinished());
        assertTrue(expected.isFinished());
    }
}
