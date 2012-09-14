/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.position;

import com.facebook.presto.Range;
import com.facebook.presto.block.Cursor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPositionsBlock {
    @Test
    public void test()
            throws Exception
    {
        PositionsBlock block = new PositionsBlock(Range.create(0, 2), Range.create(3, 5));

        Cursor cursor = block.blockCursor();

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), 0);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), 1);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), 2);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), 3);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), 4);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getPosition(), 5);

        assertFalse(cursor.advanceNextPosition());
    }
}
