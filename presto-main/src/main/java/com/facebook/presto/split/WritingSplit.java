package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class WritingSplit
        implements Split
{
    private final long shardId;

    @JsonCreator
    public WritingSplit(@JsonProperty("shardId") long shardId)
    {
        this.shardId = shardId;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.WRITING;
    }

    @JsonProperty
    public long getShardId()
    {
        return shardId;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("shardId", getShardId())
                .toString();
    }
}
