/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

public abstract class AbstractTestCursor
{
    @Test
    public void testStates()
            throws Exception
    {
        Cursor cursor = createCursor();

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
            cursor.currentValueEquals(Tuples.createTuple(0L));
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        //
        // advance to end
        //
        while (cursor.advanceNextPosition());

        //
        // We are at the last position, so all get next methods should return false
        //

        assertFalse(cursor.advanceNextValue());
        assertFalse(cursor.advanceNextPosition());
    }

    protected abstract Cursor createCursor();
}
