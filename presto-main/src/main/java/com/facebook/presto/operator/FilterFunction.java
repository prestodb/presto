/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;

public interface FilterFunction
{
    boolean filter(BlockCursor[] cursors);
}
