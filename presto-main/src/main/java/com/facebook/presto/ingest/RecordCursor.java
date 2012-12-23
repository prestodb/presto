/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import java.io.Closeable;

public interface RecordCursor
        extends Closeable
{
    boolean advanceNextPosition();

    long getLong(int field);

    double getDouble(int field);

    byte[] getString(int field);

    boolean isNull(int field);

    @Override
    void close();
}
