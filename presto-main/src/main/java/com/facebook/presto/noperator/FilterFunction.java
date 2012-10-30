/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.nblock.BlockCursor;

public interface FilterFunction
{
    boolean filter(BlockCursor[] cursors);
}
