/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestUncompressedBooleanBlockCursorWithNulls
        extends AbstractTestSingleColumnBlockCursorWithNulls
{
    @Override
    protected Block createExpectedValues()
    {
        return createBooleansBlock(null, true, null, false, null, false, null, false, null, true, null);
    }

    @Test
    public void testCursorType()
    {
        assertInstanceOf(createExpectedValues().cursor(), UncompressedBooleanBlockCursor.class);
    }
}
