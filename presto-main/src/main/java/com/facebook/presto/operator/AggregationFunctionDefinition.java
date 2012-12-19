/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.google.common.base.Preconditions;

public class AggregationFunctionDefinition
{
    public static AggregationFunctionDefinition aggregation(AggregationFunction function, int channel)
    {
        Preconditions.checkNotNull(function, "function is null");
        return new AggregationFunctionDefinition(function, channel);
    }

    private final AggregationFunction function;
    private final int channel;

    AggregationFunctionDefinition(AggregationFunction function, int channel)
    {
        this.function = function;
        this.channel = channel;
    }

    public AggregationFunction getFunction()
    {
        return function;
    }

    public int getChannel()
    {
        return channel;
    }
}
