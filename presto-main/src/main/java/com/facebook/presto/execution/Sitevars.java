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
    private final AtomicBoolean importsEnabled = new AtomicBoolean(true);

    @Inject
    public Sitevars(SitevarsConfig config)
    {
        this.importsEnabled.set(config.isImportsEnabled());
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
}
