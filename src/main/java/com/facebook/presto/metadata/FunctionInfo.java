package com.facebook.presto.metadata;

import com.facebook.presto.aggregation.AggregationFunction;

import javax.inject.Provider;

public class FunctionInfo
{
    private final boolean isAggregate;
    private final Provider<AggregationFunction> provider;

    public FunctionInfo(boolean aggregate, Provider<AggregationFunction> provider)
    {
        isAggregate = aggregate;
        this.provider = provider;
    }

    public boolean isAggregate()
    {
        return isAggregate;
    }

    public Provider<AggregationFunction> getProvider()
    {
        return provider;
    }
}
