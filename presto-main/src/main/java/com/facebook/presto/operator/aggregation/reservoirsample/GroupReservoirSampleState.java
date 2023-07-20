package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.type.Type;

public class GroupReservoirSampleState extends ReservoirSampleState{

    public GroupReservoirSampleState(Type type) {
        super(type);
    }
    public GroupReservoirSampleState(ReservoirSampleState other) {
        super(other);
    }
}
