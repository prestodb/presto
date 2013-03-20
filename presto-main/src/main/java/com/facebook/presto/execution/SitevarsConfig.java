package com.facebook.presto.execution;

import io.airlift.configuration.Config;

public class SitevarsConfig
{
    private boolean importsEnabled = true;

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
}
