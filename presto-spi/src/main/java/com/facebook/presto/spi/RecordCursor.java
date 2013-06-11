package com.facebook.presto.spi;

import java.io.Closeable;

public interface RecordCursor
        extends Closeable
{
    long getTotalBytes();

    long getCompletedBytes();

    boolean advanceNextPosition();

    boolean getBoolean(int field);

    long getLong(int field);

    double getDouble(int field);

    byte[] getString(int field);

    boolean isNull(int field);

    @Override
    void close();
}
