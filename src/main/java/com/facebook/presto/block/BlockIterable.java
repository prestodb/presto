/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

public interface BlockIterable<T extends TupleStream>
{
    BlockIterator<T> iterator(QuerySession session);
}
