/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.Tuples;
import org.testng.annotations.Test;

import java.util.Map.Entry;
import java.util.SortedMap;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextValue;
import static com.facebook.presto.block.CursorAssertions.assertNextPosition;
import static com.facebook.presto.block.CursorAssertions.assertNextValue;
import static com.facebook.presto.block.CursorAssertions.assertNextValuePosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestCursor
{
    private final SortedMap<Long,Tuple> expectedValues = CursorAssertions.toTuplesMap(createExpectedValues().cursor());

    public final Tuple getExpectedValue(long position)
    {
        return getExpectedValues().get(position);
    }

    public final SortedMap<Long, Tuple> getExpectedValues()
    {
        return expectedValues;
    }

    @Test
    public void testStates()
            throws Exception
    {
        Cursor cursor = createCursor();

        //
        // We are before the first position, so the cursor is not valid and all get current methods should throw an IllegalStateException
        //
        assertFalse(cursor.isValid());
        assertFalse(cursor.isFinished());

        try {
            cursor.getTuple();
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            cursor.currentTupleEquals(Tuples.createTuple(0L));
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        //
        // advance to end
        //
        while (Cursors.advanceNextPositionNoYield(cursor)) {
            assertTrue(cursor.isValid());
            assertFalse(cursor.isFinished());
        }

        //
        // We are beyond the last position, so the cursor is not valid, finished and all get next methods should return false
        //

        assertFalse(cursor.isValid());
        assertTrue(cursor.isFinished());

        assertAdvanceNextValue(cursor, FINISHED);
        assertAdvanceNextPosition(cursor, FINISHED);

        assertFalse(cursor.isValid());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testFirstValue()
            throws Exception
    {
        Cursor cursor = createCursor();
        assertNextValue(cursor, 0, getExpectedValue(0));
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        assertNextPosition(cursor, 0, getExpectedValue(0));
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        Cursor cursor = createCursor();

        for (Entry<Long, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextValue(cursor, entry.getKey(), entry.getValue());
        }

        assertAdvanceNextValue(cursor, FINISHED);
    }

    @Test
    public void testAdvanceNextPosition()
    {
        Cursor cursor = createCursor();

        for (Entry<Long, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextPosition(cursor, entry.getKey(), entry.getValue());
        }

        assertAdvanceNextPosition(cursor, FINISHED);
    }

    @Test
    public void testNextValuePosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        for (Long position : getExpectedValues().keySet()) {
            assertNextValuePosition(cursor, position);
        }

        assertAdvanceNextValue(cursor, FINISHED);
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();

        for (Entry<Long, Tuple> entry : getExpectedValues().entrySet()) {
            long position = entry.getKey();
            Tuple tuple = entry.getValue();
            if (position % 2 != 0) {
                assertNextValue(cursor, position, tuple);
            }
            else {
                assertNextPosition(cursor, position, tuple);
            }
        }

        assertAdvanceNextPosition(cursor, FINISHED);
        assertAdvanceNextValue(cursor, FINISHED);
    }

    @Test
    public void testGetCurrentValueEndPosition()
            throws Exception
    {
        Cursor cursor = createCursor();
        while (Cursors.advanceNextValueNoYield(cursor)) {
            assertEquals(cursor.getCurrentValueEndPosition(), cursor.getPosition());
        }
    }

    protected abstract TupleStream createExpectedValues();

    protected abstract Cursor createCursor();
}
