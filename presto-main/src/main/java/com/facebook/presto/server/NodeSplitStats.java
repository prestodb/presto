package com.facebook.presto.server;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ThriftStruct
public final class NodeSplitStats
{
    private final int totalPartitionedSplitCount;

    @ThriftConstructor
    @JsonCreator
    public NodeSplitStats(int totalPartitionedSplitCount)
    {
        this.totalPartitionedSplitCount = totalPartitionedSplitCount;
    }

    @ThriftField(1)
    @JsonProperty
    public int getTotalPartitionedSplitCount()
    {
        return totalPartitionedSplitCount;
    }
}
