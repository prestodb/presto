/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;

public interface ProjectionFunction
{
    TupleInfo getTupleInfo();

    void project(TupleReadable[] cursors, BlockBuilder output);

    void project(RecordCursor cursor, BlockBuilder output);
}
