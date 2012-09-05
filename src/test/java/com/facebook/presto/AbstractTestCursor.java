/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.slice.Slices;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;

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
            cursor.currentValueEquals(new Tuple(Slices.allocate(8), new TupleInfo(Type.FIXED_INT_64)));
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        //
        // advance to end
        //
        while(cursor.hasNextPosition()) {
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

        try {
            cursor.peekNextValuePosition();
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }

        try {
            cursor.nextValueEquals(new Tuple(Slices.allocate(8), new TupleInfo(Type.FIXED_INT_64)));
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    protected abstract Cursor createCursor();
}
