/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.sql.tree.Input;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class AggregationFunctionDefinition
{
    public static AggregationFunctionDefinition aggregation(AggregationFunction function, @Nullable Input input)
    {
        Preconditions.checkNotNull(function, "function is null");
        return new AggregationFunctionDefinition(function, input);
    }

    private final AggregationFunction function;
    private final Input input;

    AggregationFunctionDefinition(AggregationFunction function, Input input)
    {
        this.function = function;
        this.input = input;
    }

    public AggregationFunction getFunction()
    {
        return function;
    }

    public Input getInput()
    {
        return input;
    }
}
