/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.Tuples;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map.Entry;
import java.util.SortedMap;

import static com.facebook.presto.block.BlockCursorAssertions.assertAdvanceToPosition;
import static com.facebook.presto.block.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextValue;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextValuePosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestBlockCursor
{
    private final SortedMap<Integer,Tuple> expectedValues = BlockCursorAssertions.toTuplesMap(createTestCursor());

    protected abstract Block createExpectedValues();

    protected BlockCursor createTestCursor()
    {
        return createExpectedValues().cursor();
    }

    public final Tuple getExpectedValue(int position)
    {
        return getExpectedValues().get(position);
    }

    public final SortedMap<Integer, Tuple> getExpectedValues()
    {
        return expectedValues;
    }

    @BeforeClass
    public void setUp()
    {
        // verify expected values
        assertEquals(ImmutableList.copyOf(getExpectedValues().keySet()), ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    }

    @Test
    public void testAdvanceToPosition()
    {
        //
        // Note this code will more effective if the values are laid out as follows:
        //
        //   A, A, A, B, B, B, B, B, C, C, D
        //

        BlockCursor cursor = createTestCursor();

        // advance to first position
        assertAdvanceToPosition(cursor, 0);
        assertCurrentValue(cursor, 0, getExpectedValue(0));

        // skip to position in first value
        assertAdvanceToPosition(cursor, 2);
        assertCurrentValue(cursor, 2, getExpectedValue(2));

        // advance to same position
        assertAdvanceToPosition(cursor, 2);
        assertCurrentValue(cursor, 2, getExpectedValue(2));

        // skip to position in next value
        assertAdvanceToPosition(cursor, 4);
        assertCurrentValue(cursor, 4, getExpectedValue(4));

        // skip to position in third value
        assertAdvanceToPosition(cursor, 8);
        assertCurrentValue(cursor, 8, getExpectedValue(8));

        // skip to last position
        assertAdvanceToPosition(cursor, 10);
        assertCurrentValue(cursor, 10, getExpectedValue(10));

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 10, getExpectedValue(10));
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));
        assertTrue(cursor.isFinished());
        assertFalse(cursor.isValid());
    }

    @Test
    public void testStates()
    {
        BlockCursor cursor = createTestCursor();

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
        while (cursor.advanceNextPosition()) {
            assertTrue(cursor.isValid());
            assertFalse(cursor.isFinished());
        }

        //
        // We are beyond the last position, so the cursor is not valid, finished and all get next methods should return false
        //

        assertFalse(cursor.isValid());
        assertTrue(cursor.isFinished());

        assertFalse(cursor.advanceNextValue());
        assertFalse(cursor.advanceNextPosition());

        assertFalse(cursor.isValid());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testFirstValue()
    {
        BlockCursor cursor = createTestCursor();
        assertNextValue(cursor, 0, getExpectedValue(0));
    }

    @Test
    public void testFirstPosition()
    {
        BlockCursor cursor = createTestCursor();
        assertNextPosition(cursor, 0, getExpectedValue(0));
    }

    @Test
    public void testAdvanceNextValue()
    {
        BlockCursor cursor = createTestCursor();

        for (Entry<Integer, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextValue(cursor, entry.getKey(), entry.getValue());
        }

        assertFalse(cursor.advanceNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createTestCursor();

        for (Entry<Integer, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextPosition(cursor, entry.getKey(), entry.getValue());
        }

        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testNextValuePosition()
    {
        BlockCursor cursor = createTestCursor();

        for (Integer position : getExpectedValues().keySet()) {
            assertNextValuePosition(cursor, position);
        }

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testMixedValueAndPosition()
    {
        BlockCursor cursor = createTestCursor();

        for (Entry<Integer, Tuple> entry : getExpectedValues().entrySet()) {
            int position = entry.getKey();
            Tuple tuple = entry.getValue();
            if (position % 2 != 0) {
                assertNextValue(cursor, position, tuple);
            }
            else {
                assertNextPosition(cursor, position, tuple);
            }
        }

        assertFalse(cursor.advanceNextPosition());
        assertFalse(cursor.advanceNextValue());
    }

    @Test
    public void testGetCurrentValueEndPosition()
    {
        BlockCursor cursor = createTestCursor();
        while (cursor.advanceNextValue()) {
            assertEquals(cursor.getCurrentValueEndPosition(), cursor.getPosition());
        }
    }
}
