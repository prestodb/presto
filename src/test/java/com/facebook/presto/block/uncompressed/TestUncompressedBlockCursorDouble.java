/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.AbstractTestUncompressedDoubleBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlockCursor;

public class TestUncompressedBlockCursorDouble extends AbstractTestUncompressedDoubleBlockCursor
{
    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedBlockCursor(new TupleInfo(Type.DOUBLE), createTestBlock());
    }
}
