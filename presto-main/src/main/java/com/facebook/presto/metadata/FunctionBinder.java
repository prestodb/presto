package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.Input;

import javax.inject.Provider;
import java.util.List;

/**
 * Provides implementations of aggregation functions with their arguments bound to the provided inputs
 */
public interface FunctionBinder
{
    Provider<AggregationFunction> bind(List<Input> arguments);
}
