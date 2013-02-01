package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class NativeSplit
        implements Split
{
    private final long shardId;

    @JsonCreator
    public NativeSplit(@JsonProperty("shardId") long shardId)
    {
        Preconditions.checkArgument(shardId >= 0, "shard id must be at least zero");
        this.shardId = shardId;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.NATIVE;
    }

    @JsonProperty
    public long getShardId()
    {
        return shardId;
    }
}
