package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.FullAggregationFunction;
import com.facebook.presto.operator.aggregation.Input;

import javax.inject.Provider;
import java.util.List;

/**
 * Provides implementations of aggregation functions with their arguments bound to the provided inputs
 */
public interface FunctionBinder
{
    Provider<FullAggregationFunction> bind(List<Input> arguments);
}
