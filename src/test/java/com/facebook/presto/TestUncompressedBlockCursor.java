/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.operators.BlockCursor;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;

import static com.facebook.presto.BlockCursorAssertions.assertNextPosition;
import static com.facebook.presto.BlockCursorAssertions.assertNextValue;
import static com.facebook.presto.Blocks.createBlock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

public class TestUncompressedBlockCursor 
{
    @Test
    public void testStates()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        //
        // We are before the first position, so all get current methods should throw an IllegalStateException
        //
        try {
            cursor.getTuple();
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            cursor.tupleEquals(Tuples.createTuple(0L));
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        //
        // advance to end
        //
        while (cursor.hasNextPosition()) {
            cursor.advanceNextPosition();
        }

        //
        // We are at the last position, so all get next methods should throw a NoSuchElementException
        //

        try {
            cursor.advanceNextValue();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }

        try {
            cursor.advanceNextPosition();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    @Test
    public void testGetSliceState()
    {
        BlockCursor cursor = createCursor();
        try {
            cursor.getSlice(0);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testFirstValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        BlockCursorAssertions.assertNextValue(cursor, 0, "apple");
    }

    @Test
    public void testFirstPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();
        BlockCursorAssertions.assertNextPosition(cursor, 0, "apple");
    }

    @Test
    public void testAdvanceNextValue()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextValue(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextValue(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextValue(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextValue(cursor, 7, "banana");
        assertNextValue(cursor, 8, "cherry");
        assertNextValue(cursor, 9, "cherry");
        assertNextValue(cursor, 10, "date");

        assertFalse(cursor.hasNextValue());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        assertNextPosition(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextPosition(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextPosition(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextPosition(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextPosition(cursor, 8,  "cherry");
        assertNextPosition(cursor, 9,  "cherry");
        assertNextPosition(cursor, 10, "date");

        assertFalse(cursor.hasNextPosition());
    }

    @Test
    public void testAdvanceToNextValueAdvancesPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        // first, skip to middle of a block
        BlockCursorAssertions.assertNextValue(cursor, 0, "apple");
        BlockCursorAssertions.assertNextPosition(cursor, 1, "apple");

        // force jump to next block
        BlockCursorAssertions.assertNextValue(cursor, 2, "apple");
    }

    @Test
    public void testMixedValueAndPosition()
            throws Exception
    {
        BlockCursor cursor = createCursor();

        assertNextValue(cursor, 0, "apple");
        assertNextPosition(cursor, 1, "apple");
        assertNextValue(cursor, 2, "apple");
        assertNextPosition(cursor, 3, "banana");
        assertNextValue(cursor, 4, "banana");
        assertNextPosition(cursor, 5, "banana");
        assertNextValue(cursor, 6, "banana");
        assertNextPosition(cursor, 7, "banana");
        assertNextValue(cursor, 8, "cherry");
        assertNextPosition(cursor, 9, "cherry");
        assertNextValue(cursor, 10, "date");

        assertFalse(cursor.hasNextPosition());
        assertFalse(cursor.hasNextValue());
    }

    protected BlockCursor createCursor()
    {
        return createBlock(0, "apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date").blockCursor();
    }
}
