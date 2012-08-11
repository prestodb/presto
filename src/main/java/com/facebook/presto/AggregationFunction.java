package com.facebook.presto;

public interface AggregationFunction
{
    void add(ValueBlock values, PositionBlock relevantPositions);
    Object evaluate();
}
