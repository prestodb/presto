package com.facebook.presto.connector.dual;

import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;

import javax.inject.Inject;

public class DualConnector
        implements Connector
{
    private final ClassToInstanceMap<Object> services;

    @Inject
    public DualConnector(NodeManager nodeManager)
    {
        ImmutableClassToInstanceMap.Builder<Object> services = ImmutableClassToInstanceMap.builder();
        services.put(ConnectorMetadata.class, new DualMetadata());
        services.put(ConnectorSplitManager.class, new DualSplitManager(nodeManager));
        services.put(ConnectorDataStreamProvider.class, new DualDataStreamProvider());
        services.put(ConnectorHandleResolver.class, new DualHandleResolver());

        this.services = services.build();
    }

    @Override
    public <T> T getService(Class<T> type)
    {
        return services.getInstance(type);
    }

}
