/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.tuple.TupleInfo;

public interface BlockEncoding
{
    /**
     * Gets the TupleInfo this encoding supports.
     */
    TupleInfo getTupleInfo();

    /**
     * Read a block from the specified input.  The returned
     * block should begin at the specified position.
     */
    Block readBlock(SliceInput input, long positionOffset);

    /**
     * Write the specified block to the specified output
     */
    void writeBlock(SliceOutput sliceOutput, Block block);
}
