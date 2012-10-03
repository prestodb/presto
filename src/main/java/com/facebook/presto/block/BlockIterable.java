/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

public interface BlockIterable<T extends TupleStream> extends Iterable<T> {
    @Override
    BlockIterator<T> iterator();
}
