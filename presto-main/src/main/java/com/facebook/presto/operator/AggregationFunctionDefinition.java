/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.NewAggregationFunction;
import com.google.common.base.Preconditions;

public class AggregationFunctionDefinition
{
    public static AggregationFunctionDefinition aggregation(NewAggregationFunction function, int channel)
    {
        Preconditions.checkNotNull(function, "function is null");
        return new AggregationFunctionDefinition(function, channel);
    }

    private final NewAggregationFunction function;
    private final int channel;

    AggregationFunctionDefinition(NewAggregationFunction function, int channel)
    {
        this.function = function;
        this.channel = channel;
    }

    public NewAggregationFunction getFunction()
    {
        return function;
    }

    public int getChannel()
    {
        return channel;
    }
}
