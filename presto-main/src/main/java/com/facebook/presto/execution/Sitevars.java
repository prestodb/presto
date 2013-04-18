package com.facebook.presto.execution;

import org.weakref.jmx.Managed;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Presto 'site vars'.
 */
@Singleton
public class Sitevars
{
    private final AtomicBoolean importsEnabled;
    private final AtomicBoolean dropEnabled;
    private final AtomicBoolean shardCleaningEnabled;
    private final AtomicBoolean aliasEnabled;

    @Inject
    public Sitevars(SitevarsConfig config)
    {
        this.importsEnabled = new AtomicBoolean(config.isImportsEnabled());
        this.dropEnabled = new AtomicBoolean(config.isDropEnabled());
        this.shardCleaningEnabled = new AtomicBoolean(config.isShardCleaningEnabled());
        this.aliasEnabled = new AtomicBoolean(config.isAliasEnabled());
    }

    @Managed
    public boolean isImportsEnabled()
    {
        return importsEnabled.get();
    }

    @Managed
    public Sitevars setImportsEnabled(boolean importsEnabled)
    {
        this.importsEnabled.set(importsEnabled);
        return this;
    }

    @Managed
    public boolean isDropEnabled()
    {
        return dropEnabled.get();
    }

    @Managed
    public Sitevars setDropEnabled(boolean dropEnabled)
    {
        this.dropEnabled.set(dropEnabled);
        return this;
    }

    @Managed
    public boolean isShardCleaningEnabled()
    {
        return shardCleaningEnabled.get();
    }

    @Managed
    public Sitevars setShardCleaningEnabled(boolean shardCleaningEnabled)
    {
        this.shardCleaningEnabled.set(shardCleaningEnabled);
        return this;
    }

    @Managed
    public boolean isAliasEnabled()
    {
        return aliasEnabled.get();
    }

    @Managed
    public Sitevars setAliasEnabled(boolean aliasEnabled)
    {
        this.aliasEnabled.set(aliasEnabled);
        return this;
    }
}
