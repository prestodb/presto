package com.facebook.presto.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class ShardCleanerConfig
{
    private boolean enabled = false;
    private Duration storageCleanerInterval = new Duration(60, TimeUnit.SECONDS);
    private int maxShardDropThreads = 32;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("shard-cleaner.enabled")
    @ConfigDescription("Run the periodic importer")
    public ShardCleanerConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @MinDuration("60s")
    @NotNull
    public Duration getCleanerInterval()
    {
        return storageCleanerInterval;
    }

    @Config("shard-cleaner.interval")
    public ShardCleanerConfig setCleanerInterval(Duration storageCleanerInterval)
    {
        this.storageCleanerInterval = storageCleanerInterval;
        return this;
    }

    @Min(1)
    public int getMaxThreads()
    {
        return maxShardDropThreads;
    }

    @Config("shard-cleaner.max-threads")
    public ShardCleanerConfig setMaxThreads(int maxShardDropThreads)
    {
        this.maxShardDropThreads = maxShardDropThreads;
        return this;
    }
}