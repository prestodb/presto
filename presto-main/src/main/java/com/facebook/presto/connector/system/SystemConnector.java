package com.facebook.presto.connector.system;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;

import javax.inject.Inject;

public class SystemConnector
        implements Connector
{
    private final ClassToInstanceMap<Object> services;

    @Inject
    public SystemConnector(SystemTablesMetadata value, SystemSplitManager systemSplitManager, SystemDataStreamProvider systemDataStreamProvider)
    {
        ImmutableClassToInstanceMap.Builder<Object> services = ImmutableClassToInstanceMap.builder();
        services.put(ConnectorMetadata.class, value);
        services.put(ConnectorSplitManager.class, systemSplitManager);
        services.put(ConnectorDataStreamProvider.class, systemDataStreamProvider);
        services.put(ConnectorHandleResolver.class, new SystemHandleResolver());

        this.services = services.build();
    }

    @Override
    public <T> T getService(Class<T> type)
    {
        return services.getInstance(type);
    }

}
