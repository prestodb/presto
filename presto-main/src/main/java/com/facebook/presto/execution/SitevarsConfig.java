package com.facebook.presto.execution;

import io.airlift.configuration.Config;

public class SitevarsConfig
{
    private boolean importsEnabled = true;
    private boolean dropEnabled = true;
    private boolean shardCleaningEnabled = true;

    public boolean isImportsEnabled()
    {
        return importsEnabled;
    }

    @Config("sitevar.imports-enabled")
    public SitevarsConfig setImportsEnabled(boolean importsEnabled)
    {
        this.importsEnabled = importsEnabled;
        return this;
    }

    public boolean isDropEnabled()
    {
        return dropEnabled;
    }

    @Config("sitevar.drop-enabled")
    public SitevarsConfig setDropEnabled(boolean dropEnabled)
    {
        this.dropEnabled = dropEnabled;
        return this;
    }

    public boolean isShardCleaningEnabled()
    {
        return shardCleaningEnabled;
    }

    @Config("sitevar.shard-cleaning-enabled")
    public SitevarsConfig setShardCleaningEnabled(boolean shardCleaningEnabled)
    {
        this.shardCleaningEnabled = shardCleaningEnabled;
        return this;
    }
}
