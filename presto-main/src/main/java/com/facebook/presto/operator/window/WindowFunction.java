/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public interface WindowFunction
{
    TupleInfo getTupleInfo();

    /**
     * Reset state for a new partition (including the first one).
     *
     * @param partitionRowCount the total number of rows in the new partition
     */
    void reset(int partitionRowCount);

    /**
     * Process a row by outputting the result of the window function.
     * <p/>
     * This method provides information about the ordering peer group. A peer group is all
     * of the rows that are peers within the specified ordering. Rows are peers if they
     * compare equal to each other using the specified ordering expression. The ordering
     * of rows within a peer group is undefined (otherwise they would not be peers).
     *
     * @param newPeerGroup if this row starts a new peer group
     * @param peerGroupCount the total number of rows in this peer group
     */
    void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount);
}
