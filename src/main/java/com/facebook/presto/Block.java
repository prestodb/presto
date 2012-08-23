package com.facebook.presto;



public interface Block
{
    boolean isEmpty();
    int getCount();

    boolean isSorted();
    boolean isSingleValue();
    boolean isPositionsContiguous();

    Iterable<Long> getPositions();

    Range getRange();
}
