package com.facebook.presto.metadata;

import com.facebook.presto.spi.connector.ConnectorPartitionHandle;

import javax.inject.Inject;

public class PartitionHandleJacksonModule
        extends AbstractTypedJacksonModule<ConnectorPartitionHandle>
{
    @Inject
    public PartitionHandleJacksonModule(HandleResolver handleResolver)
    {
        super(ConnectorPartitionHandle.class,
                handleResolver::getId,
                handleResolver::getPartitionHandleClass);
    }
}
