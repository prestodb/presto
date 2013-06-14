/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.sql.tree.Input;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

public class AggregationFunctionDefinition
{
    public static AggregationFunctionDefinition aggregation(AggregationFunction function, List<Input> inputs)
    {
        Preconditions.checkNotNull(function, "function is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return new AggregationFunctionDefinition(function, inputs);
    }

    public static AggregationFunctionDefinition aggregation(AggregationFunction function, Input... inputs)
    {
        Preconditions.checkNotNull(function, "function is null");
        Preconditions.checkNotNull(inputs, "inputs is null");

        return aggregation(function, Arrays.asList(inputs));
    }

    private final AggregationFunction function;
    private final List<Input> inputs;

    AggregationFunctionDefinition(AggregationFunction function, List<Input> inputs)
    {
        this.function = function;
        this.inputs = inputs;
    }

    public AggregationFunction getFunction()
    {
        return function;
    }

    public List<Input> getInputs()
    {
        return inputs;
    }
}
