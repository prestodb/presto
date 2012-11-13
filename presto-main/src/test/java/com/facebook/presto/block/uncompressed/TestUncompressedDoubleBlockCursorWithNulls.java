/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedDoubleBlockCursorWithNulls
        extends AbstractTestSingleColumnBlockCursorWithNulls
{
    @Override
    protected Block createExpectedValues()
    {
        return createDoublesBlock(null, 11.11, null, 22.22, null, 22.22, null, 22.22, null, 33.33, null);
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedDoubleBlockCursor.class);
    }
}
