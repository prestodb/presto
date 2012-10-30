/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;

public interface ProjectionFunction
{
    TupleInfo getTupleInfo();

    void project(BlockCursor[] cursors, BlockBuilder output);

    void project(Tuple[] tuples, BlockBuilder output);
}
