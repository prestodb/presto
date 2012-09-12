/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;

public class TestUncompressedBlockCursorDouble extends AbstractTestUncompressedDoubleBlockCursor
{
    @Override
    protected BlockCursor createCursor()
    {
        return new UncompressedBlockCursor(new TupleInfo(Type.DOUBLE), createTestBlock());
    }
}
