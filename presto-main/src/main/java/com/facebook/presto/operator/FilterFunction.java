/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.tuple.TupleReadable;

public interface FilterFunction
{
    boolean filter(TupleReadable... cursors);

    boolean filter(RecordCursor cursor);
}
