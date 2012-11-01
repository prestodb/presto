/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

public interface RecordIterable extends Iterable<Record> {
    RecordIterator iterator();
}
