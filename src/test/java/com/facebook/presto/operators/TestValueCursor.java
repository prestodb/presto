/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operators;

import com.facebook.presto.AbstractTestUncompressedSliceCursor;
import com.facebook.presto.Cursor;

public class TestValueCursor
    extends AbstractTestUncompressedSliceCursor
{
    @Override
    protected Cursor createCursor()
    {
        return new ValueCursor(createTupleInfo(), createBlocks().iterator());
    }
}
