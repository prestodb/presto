package com.facebook.presto.execution;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class QueryManagerConfig
{
    private boolean importsEnabled = true;
    private DataSize maxOperatorMemoryUsage = new DataSize(256, Unit.MEGABYTE);
    private int maxShardProcessorThreads = Runtime.getRuntime().availableProcessors() * 4;

    public boolean isImportsEnabled()
    {
        return importsEnabled;
    }

    @Config("import.enabled")
    public QueryManagerConfig setImportsEnabled(boolean importsEnabled)
    {
        this.importsEnabled = importsEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxOperatorMemoryUsage()
    {
        return maxOperatorMemoryUsage;
    }

    @Config("query.operator.max-memory")
    public QueryManagerConfig setMaxOperatorMemoryUsage(DataSize maxOperatorMemoryUsage)
    {
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;
        return this;
    }

    @Min(1)
    public int getMaxShardProcessorThreads()
    {
        return maxShardProcessorThreads;
    }

    @Config("query.shard.max-threads")
    public QueryManagerConfig setMaxShardProcessorThreads(int maxShardProcessorThreads)
    {
        this.maxShardProcessorThreads = maxShardProcessorThreads;
        return this;
    }
}
