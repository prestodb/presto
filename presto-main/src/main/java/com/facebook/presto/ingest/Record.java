/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

public interface Record
{
    int getFieldCount();

    Long getLong(int field);

    Double getDouble(int field);

    String getString(int field);
}
