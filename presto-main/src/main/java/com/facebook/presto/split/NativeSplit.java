package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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
