package com.facebook.presto;

import com.google.common.collect.Range;

public interface Block
{
    boolean isEmpty();
    int getCount();

    boolean isSorted();
    boolean isSingleValue();
    boolean isPositionsContiguous();

    Iterable<Long> getPositions();

    Range<Long> getRange();
}
