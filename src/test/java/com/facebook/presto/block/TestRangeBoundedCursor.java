package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Blocks.assertCursorsEquals;
import static com.facebook.presto.block.Blocks.createBlock;
import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static org.testng.Assert.*;

public class TestRangeBoundedCursor extends AbstractTestNonContiguousCursor
{
    @Test
    public void testFullRange() throws Exception
    {
        // Total Range: 0-7
        TupleStream originalTupleStream = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb");
        Cursor cursor = originalTupleStream.cursor();
        assertCursorsEquals(
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
        assertCursorsEquals(
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
        assertCursorsEquals(
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
        assertCursorsEquals(
                RangeBoundedCursor.bound(cursor, Range.create(3, 6)),
                Blocks.createTupleStream(3, "d", "e", "e", "a").cursor()
        );
    }

    @Test
    public void testEndRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertCursorsEquals(
                RangeBoundedCursor.bound(cursor, Range.create(6, 7)),
                Blocks.createTupleStream(6, "a", "bb").cursor()
        );
    }

    @Test
    public void testPreinitAndBeforeStart() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(0, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertAdvanceNextPosition(cursor);
        assertCursorsEquals(
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
        assertAdvanceNextPosition(cursor);
        assertAdvanceNextPosition(cursor);
        assertAdvanceNextPosition(cursor); // Cursor is at position 2
        assertCursorsEquals(
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
        assertAdvanceNextPosition(cursor);
        Cursor rangedCursor = RangeBoundedCursor.bound(cursor, Range.create(10, 13));
        assertAdvanceNextPosition(rangedCursor, FINISHED);
        assertTrue(rangedCursor.isFinished());
    }

    @Test
    public void testPreinitAndBeforeRange() throws Exception
    {
        // Total Range: 0-7
        Cursor cursor = Blocks.createTupleStream(10, "a", "bb", "c", "d", "e", "e", "a", "bb").cursor();
        assertAdvanceNextPosition(cursor);
        Cursor rangedCursor = RangeBoundedCursor.bound(cursor, Range.create(0, 1));
        assertAdvanceNextPosition(rangedCursor, FINISHED);
        assertTrue(rangedCursor.isFinished());
    }

    private TupleStream createBaseTupleStream()
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_VARBINARY, ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "banana", "banana"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "cherry", "cherry"),
                createBlock(30, "date"),
                createBlock(40, "date"),
                createBlock(50, "apple", "banana")
        ));
    }
    
    private TupleStream createExpectedTupleStream()
    {
        return new GenericTupleStream<>(TupleInfo.SINGLE_VARBINARY, ImmutableList.of(
                createBlock(0, "apple", "apple", "apple", "banana", "banana"),
                createBlock(5, "banana", "banana", "banana"),
                createBlock(20, "cherry", "cherry"),
                createBlock(30, "date")
        ));
    }

    @Override
    protected TupleStream createExpectedValues()
    {
        return createExpectedTupleStream();
    }

    @Override
    protected Cursor createCursor()
    {
        return new RangeBoundedCursor(Range.create(0, 30), createBaseTupleStream().cursor());
    }
}
