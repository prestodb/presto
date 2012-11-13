/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import org.testng.annotations.Test;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBlockBuilder
{
    @Test
    public void testMultipleTuplesWithNull()
    {
        BlockCursor cursor = new BlockBuilder(SINGLE_LONG).appendNull()
                .append(42)
                .appendNull()
                .append(42)
                .build()
                .cursor();

        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.isNull(0));

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 42L);

        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.isNull(0));

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 42L);
    }
}
