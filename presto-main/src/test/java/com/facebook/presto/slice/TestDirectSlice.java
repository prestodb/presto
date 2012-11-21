/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.slice;

import java.nio.ByteBuffer;

public class TestDirectSlice
        extends TestSlice
{
    @Override
    protected Slice allocate(int size)
    {
        if (size == 0) {
            return Slices.EMPTY_SLICE;
        }
        return Slice.toUnsafeSlice(ByteBuffer.allocateDirect(size));
    }
}
