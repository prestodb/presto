/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.tuple.TupleInfo;

public interface BlockIterable extends Iterable<Block>
{
    TupleInfo getTupleInfo();
}
