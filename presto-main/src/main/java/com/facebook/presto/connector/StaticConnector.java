package com.facebook.presto.connector;

import com.facebook.presto.spi.Connector;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;

import java.util.Map;

public class StaticConnector
        implements Connector
{
    private final ClassToInstanceMap<Object> services;

    public StaticConnector(Map<Class<?>, ?> services)
    {
        this.services = ImmutableClassToInstanceMap.copyOf(services);
    }

    @Override
    public <T> T getService(Class<T> type)
    {
        return services.getInstance(type);
    }
}
