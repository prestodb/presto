/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.operator.GenericCursor;

public class TestGenericCursor
    extends AbstractTestUncompressedSliceCursor
{
    @Override
    protected Cursor createCursor()
    {
        return new GenericCursor(createTupleInfo(), createBlocks().iterator());
    }
}
