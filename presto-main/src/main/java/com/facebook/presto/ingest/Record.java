/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

public interface Record
{
    int getFieldCount();

    long getLong(int field);

    double getDouble(int field);

    byte[] getString(int field);

    boolean isNull(int field);
}
