/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import io.airlift.units.DataSize;

public interface BlockIterable extends Iterable<Block>
{
    TupleInfo getTupleInfo();

    Optional<DataSize> getDataSize();

    Optional<Integer> getPositionCount();
}
