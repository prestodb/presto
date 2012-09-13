/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Tuples;
import com.facebook.presto.block.BlockCursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestUncompressedBlockCursor
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
        while (cursor.advanceNextPosition());

        //
        // We are at the last position, so all get next methods should throw a NoSuchElementException
        //

        assertFalse(cursor.advanceToNextValue());
        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createCursor();

        assertTrue(cursor.advanceToNextValue());
        assertTrue(cursor.advanceNextPosition());

        assertTrue(cursor.advanceToNextValue());
        assertTrue(cursor.advanceNextPosition());
    }

    protected abstract BlockCursor createCursor();
}
