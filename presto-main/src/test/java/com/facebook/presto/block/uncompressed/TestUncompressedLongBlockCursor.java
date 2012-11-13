/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedLongBlockCursor extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createLongsBlock(1111L, 1111L, 1111L, 2222L, 2222L, 2222L, 2222L, 2222L, 3333L, 3333L, 4444L);
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedLongBlockCursor.class);
    }
}
