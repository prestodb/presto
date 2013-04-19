package com.facebook.presto.tpch;

import com.facebook.presto.connector.StaticConnector;
import com.facebook.presto.metadata.ConnectorHandleResolver;
import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.ConnectorSplitManager;
import com.google.common.collect.ImmutableClassToInstanceMap;

import javax.inject.Inject;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TpchConnectorFactory
        implements ConnectorFactory
{
    private static final TpchHandleResolver HANDLE_RESOLVER = new TpchHandleResolver();

    private final TpchMetadata tpchMetadata;
    private final TpchSplitManager splitManager;
    private final TpchDataStreamProvider dataStreamProvider;

    @Inject
    public TpchConnectorFactory(TpchMetadata tpchMetadata, TpchSplitManager splitManager, TpchDataStreamProvider dataStreamProvider)
    {
        this.tpchMetadata = checkNotNull(tpchMetadata, "tpchMetadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {

        ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
        builder.put(ConnectorMetadata.class, tpchMetadata);
        builder.put(ConnectorSplitManager.class, splitManager);
        builder.put(ConnectorDataStreamProvider.class, dataStreamProvider);
        builder.put(ConnectorHandleResolver.class, HANDLE_RESOLVER);

        return new StaticConnector(builder.build());
    }
}
