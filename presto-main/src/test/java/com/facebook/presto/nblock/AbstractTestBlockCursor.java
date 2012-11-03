/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.Tuples;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map.Entry;
import java.util.SortedMap;

import static com.facebook.presto.nblock.BlockCursorAssertions.assertAdvanceToPosition;
import static com.facebook.presto.nblock.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.nblock.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.nblock.BlockCursorAssertions.assertNextValue;
import static com.facebook.presto.nblock.BlockCursorAssertions.assertNextValuePosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestBlockCursor
{
    private final SortedMap<Long,Tuple> expectedValues = BlockCursorAssertions.toTuplesMap(createTestCursor());

    protected abstract Block createExpectedValues();

    protected BlockCursor createTestCursor()
    {
        return createExpectedValues().cursor();
    }

    public final Tuple getExpectedValue(long position)
    {
        return getExpectedValues().get(position);
    }

    public final SortedMap<Long, Tuple> getExpectedValues()
    {
        return expectedValues;
    }

    @BeforeClass
    public void setUp()
    {
        // verify expected values
        assertEquals(ImmutableList.copyOf(getExpectedValues().keySet()), ImmutableList.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
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
        assertFalse(cursor.advanceToPosition((long) 100));
        assertTrue(cursor.isFinished());
        assertFalse(cursor.isValid());
    }

    @Test
    public void testRange()
    {
        BlockCursor cursor = createTestCursor();
        Assert.assertEquals(cursor.getRange(), new Range(0, 10));
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

        for (Entry<Long, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextValue(cursor, entry.getKey(), entry.getValue());
        }

        assertFalse(cursor.advanceNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createTestCursor();

        for (Entry<Long, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextPosition(cursor, entry.getKey(), entry.getValue());
        }

        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testNextValuePosition()
    {
        BlockCursor cursor = createTestCursor();

        for (Long position : getExpectedValues().keySet()) {
            assertNextValuePosition(cursor, position);
        }

        assertFalse(cursor.advanceNextValue());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testMixedValueAndPosition()
    {
        BlockCursor cursor = createTestCursor();

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
