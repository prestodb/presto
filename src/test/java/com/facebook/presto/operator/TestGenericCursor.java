/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.AbstractTestUncompressedSliceCursor;
import com.facebook.presto.block.Cursor;

public class TestGenericCursor
    extends AbstractTestUncompressedSliceCursor
{
    @Override
    protected Cursor createCursor()
    {
        return new GenericCursor(createTupleInfo(), createBlocks().iterator());
    }
}
