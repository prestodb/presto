/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockBuilder;

public interface RecordProjection {
    TupleInfo getTupleInfo();

    void project(Record record, BlockBuilder output);
}
