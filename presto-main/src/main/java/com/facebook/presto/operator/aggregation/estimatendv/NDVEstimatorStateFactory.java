package com.facebook.presto.operator.aggregation.estimatendv;

import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class NDVEstimatorStateFactory
        implements AccumulatorStateFactory<NDVEstimatorState>
{

    public NDVEstimatorStateFactory() {}

    @Override
    public NDVEstimatorState createSingleState()
    {
        return new SingleNDVEstimatorState();
    }

    @Override
    public Class<? extends NDVEstimatorState> getSingleStateClass()
    {
        return SingleNDVEstimatorState.class;
    }

    @Override
    public NDVEstimatorState createGroupedState()
    {
        return new GroupNDVEstimatorState();
    }

    @Override
    public Class<? extends NDVEstimatorState> getGroupedStateClass()
    {
        return GroupNDVEstimatorState.class;
    }
}
