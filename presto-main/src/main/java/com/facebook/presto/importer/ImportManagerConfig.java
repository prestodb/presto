package com.facebook.presto.importer;

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class ImportManagerConfig
{
    private int maxPartitionThreads = 50;
    private int maxChunkThreads = 50;
    private int maxShardThreads = 50;

    @Min(1)
    public int getMaxPartitionThreads()
    {
        return maxPartitionThreads;
    }

    @Config("import.max-partition-threads")
    public ImportManagerConfig setMaxPartitionThreads(int maxPartitionThreads)
    {
        this.maxPartitionThreads = maxPartitionThreads;
        return this;
    }

    @Min(1)
    public int getMaxChunkThreads()
    {
        return maxChunkThreads;
    }

    @Config("import.max-chunk-threads")
    public ImportManagerConfig setMaxChunkThreads(int maxChunkThreads)
    {
        this.maxChunkThreads = maxChunkThreads;
        return this;
    }

    @Min(1)
    public int getMaxShardThreads()
    {
        return maxShardThreads;
    }

    @Config("import.max-shard-threads")
    public ImportManagerConfig setMaxShardThreads(int maxShardThreads)
    {
        this.maxShardThreads = maxShardThreads;
        return this;
    }
}
