package com.facebook.presto.execution;

import io.airlift.configuration.Config;

public class QueryManagerConfig
{
    private boolean importsEnabled = true;

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
}
