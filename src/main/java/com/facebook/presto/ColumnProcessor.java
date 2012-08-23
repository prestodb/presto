/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;

public interface ColumnProcessor
{
    Type getColumnType();

    void processBlock(ValueBlock block);

    void finish();
}
