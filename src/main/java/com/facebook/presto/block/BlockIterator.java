/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.google.common.collect.PeekingIterator;

public interface BlockIterator<T extends TupleStream> extends PeekingIterator<T>
{
    boolean mustYield();
    boolean canAdvance();
}
