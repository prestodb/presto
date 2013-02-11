/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public interface WindowFunction
{
    TupleInfo getTupleInfo();

    void reset();

    void processRow(BlockBuilder output);
}
