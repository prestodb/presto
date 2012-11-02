/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock.uncompressed;

import com.facebook.presto.nblock.AbstractTestBlockCursor;
import com.facebook.presto.nblock.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.nblock.BlockAssertions.createStringsBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedSliceBlockCursor extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock(0, "apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedSliceBlockCursor.class);
    }
}
