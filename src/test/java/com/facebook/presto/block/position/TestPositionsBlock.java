/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.position;

import com.facebook.presto.Range;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import org.testng.annotations.Test;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.CursorAssertions.assertAdvanceNextPosition;
import static org.testng.Assert.assertEquals;

public class TestPositionsBlock {
    @Test
    public void test()
            throws Exception
    {
        PositionsBlock block = new PositionsBlock(Range.create(0, 2), Range.create(3, 5));

        Cursor cursor = block.cursor(new QuerySession());

        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), 0);

        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), 1);

        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), 2);

        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), 3);

        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), 4);

        assertAdvanceNextPosition(cursor);
        assertEquals(cursor.getPosition(), 5);

        assertAdvanceNextPosition(cursor, FINISHED);
    }
}
