/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Tuples;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestCursor
{
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

    protected abstract Cursor createCursor();
}
