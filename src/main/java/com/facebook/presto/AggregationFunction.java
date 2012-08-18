package com.facebook.presto;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(ValueBlock values, PositionBlock relevantPositions);

    Tuple evaluate();
}
