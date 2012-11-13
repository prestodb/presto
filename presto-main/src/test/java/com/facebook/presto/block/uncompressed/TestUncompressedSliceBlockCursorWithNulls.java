/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedSliceBlockCursorWithNulls
        extends AbstractTestSingleColumnBlockCursorWithNulls
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(null, "apple", null, "banana", null, "banana", null, "banana", null, "cherry", null);
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedSliceBlockCursor.class);
    }

}
