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
    private int maxNumberOfGroups = 1_000_000;

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
    public int getMaxNumberOfGroups()
    {
        return maxNumberOfGroups;
    }

    @Config("query.group-by.max-group-count")
    public QueryManagerConfig setMaxNumberOfGroups(int maxNumberOfGroups)
    {
        this.maxNumberOfGroups = maxNumberOfGroups;
        return this;
    }
}
