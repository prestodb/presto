package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.reservoirsample.ReservoirSampleState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.lang.String.format;

public class ReservoirSampleStateFactory
        implements AccumulatorStateFactory<ReservoirSampleState>
{
    private final Type type;

    public ReservoirSampleStateFactory(Type type)
    {
        this.type = type;
    }

    @Override
    public ReservoirSampleState createSingleState()
    {
        return new SingleReservoirSampleState(type);
    }

    @Override
    public Class<? extends ReservoirSampleState> getSingleStateClass()
    {
        return SingleReservoirSampleState.class;
    }

    @Override
    public ReservoirSampleState createGroupedState()
    {
        return new GroupReservoirSampleState(type);
    }

    @Override
    public Class<? extends ReservoirSampleState> getGroupedStateClass()
    {
        return GroupReservoirSampleState.class;
    }
}
