package com.facebook.presto.hive;

import com.facebook.presto.spi.Connector;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;

import java.util.Map;

public class HiveConnector
        implements Connector
{
    private final ClassToInstanceMap<Object> services;

    public HiveConnector(Map<Class<?>, ?> services)
    {
        this.services = ImmutableClassToInstanceMap.copyOf(services);
    }

    @Override
    public <T> T getService(Class<T> type)
    {
        return services.getInstance(type);
    }
}
