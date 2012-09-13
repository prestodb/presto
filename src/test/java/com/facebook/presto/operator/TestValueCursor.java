/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.AbstractTestUncompressedSliceCursor;
import com.facebook.presto.block.Cursor;

public class TestValueCursor
    extends AbstractTestUncompressedSliceCursor
{
    @Override
    protected Cursor createCursor()
    {
        return new ValueCursor(createTupleInfo(), createBlocks().iterator());
    }
}
