/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

public interface YieldingIterable<T extends TupleStream>
{
    YieldingIterator<T> iterator(QuerySession session);
}
