package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.type.Type;

public class SingleReservoirSampleState extends ReservoirSampleState {
    public SingleReservoirSampleState(Type type) {
        super(type);
    }
    public SingleReservoirSampleState(ReservoirSampleState other) {
        super(other);
    }

}
