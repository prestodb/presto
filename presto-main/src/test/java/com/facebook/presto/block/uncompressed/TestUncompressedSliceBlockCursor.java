/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.AbstractTestBlockCursor;
import com.facebook.presto.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedSliceBlockCursor extends AbstractTestBlockCursor
{
    @Override
    protected Block createExpectedValues()
    {
        return createStringsBlock("apple", "apple", "apple", "banana", "banana", "banana", "banana", "banana", "cherry", "cherry", "date");
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedSliceBlockCursor.class);
    }
}
