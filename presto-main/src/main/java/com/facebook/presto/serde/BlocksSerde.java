/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

public interface BlocksSerde
{
    BlockIterable createBlocksReader(Slice slice, long positionOffset);

    BlocksWriter createBlocksWriter(SliceOutput sliceOutput);
}
