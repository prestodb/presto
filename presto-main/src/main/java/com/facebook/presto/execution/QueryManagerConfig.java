package com.facebook.presto.execution;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.validation.constraints.NotNull;

public class QueryManagerConfig
{
    private boolean importsEnabled = true;
    private DataSize maxOperatorMemoryUsage = new DataSize(256, Unit.MEGABYTE);

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
}
