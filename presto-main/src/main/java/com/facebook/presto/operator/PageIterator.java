/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.PeekingIterator;

import java.io.Closeable;
import java.util.List;

public interface PageIterator
        extends PeekingIterator<Page>, Closeable
{
    int getChannelCount();

    List<TupleInfo> getTupleInfos();

    @Override
    void close();
}
