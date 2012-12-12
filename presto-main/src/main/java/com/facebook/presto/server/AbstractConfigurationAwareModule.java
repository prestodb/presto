package com.facebook.presto.server;

import com.google.inject.Binder;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
abstract class AbstractConfigurationAwareModule
        implements ConfigurationAwareModule
{
    protected ConfigurationFactory configurationFactory;
    protected Binder binder;

    @Override
    public synchronized void setConfigurationFactory(ConfigurationFactory configurationFactory)
    {
        this.configurationFactory = checkNotNull(configurationFactory, "configurationFactory is null");
    }

    @SuppressWarnings("ParameterHidesMemberVariable")
    @Override
    public final synchronized void configure(Binder binder)
    {
        checkState(this.binder == null, "re-entry not allowed");
        this.binder = checkNotNull(binder, "binder is null");
        try {
            configure();
        }
        finally {
            this.binder = null;
        }
    }

    protected synchronized void install(ConfigurationAwareModule module)
    {
        module.setConfigurationFactory(configurationFactory);
        binder.install(module);
    }

    protected abstract void configure();
}
