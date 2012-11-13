/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import org.testng.annotations.Test;

import java.util.Map.Entry;

import static com.facebook.presto.block.BlockCursorAssertions.assertNextPosition;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestSingleColumnBlockCursorWithNulls
        extends AbstractTestBlockCursor
{
    @Test
    public void testNullValues()
    {
        BlockCursor cursor = createTestCursor();

        for (Entry<Integer, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextPosition(cursor, entry.getKey(), entry.getValue());
            if (cursor.getPosition() % 2 == 0) {
                assertTrue(cursor.isNull(0));
            }
        }

        assertFalse(cursor.advanceNextPosition());
    }
}
