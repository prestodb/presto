/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import java.io.Closeable;
import java.util.Iterator;

public interface RecordIterator
        extends Iterator<Record>, Closeable
{
    @Override
    void close();
}
