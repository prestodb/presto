/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.primitives.Longs;
import org.testng.annotations.Test;

import static com.facebook.presto.block.cursor.BlockCursorAssertions.assertNextValue;
import static org.testng.Assert.assertFalse;

public class TestMaskedValueBlock
{
    @Test
    public void test()
            throws Exception
    {
        UncompressedValueBlock uncompressed = Blocks.createBlock(0, "a", "b", "c", "d", "e", "f");

        MaskedValueBlock block = new MaskedValueBlock(uncompressed, Longs.asList(0, 2, 4));

        BlockCursor cursor = block.blockCursor();

        assertNextValue(cursor, 0, "a");
        assertNextValue(cursor, 2, "c");
        assertNextValue(cursor, 4, "e");

        assertFalse(cursor.advanceNextPosition());
    }
}
