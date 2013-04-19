package com.facebook.presto.tpch;

import com.facebook.presto.connector.StaticConnector;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.collect.ImmutableClassToInstanceMap;

import javax.inject.Inject;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TpchConnectorFactory
        implements ConnectorFactory
{
    private static final TpchHandleResolver HANDLE_RESOLVER = new TpchHandleResolver();

    private final NodeManager nodeManager;
    private final TpchBlocksProvider tpchBlocksProvider;

    @Inject
    public TpchConnectorFactory(NodeManager nodeManager, TpchBlocksProvider tpchBlocksProvider)
    {
        this.tpchBlocksProvider = tpchBlocksProvider;
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
        builder.put(ConnectorMetadata.class, new TpchMetadata());
        builder.put(ConnectorSplitManager.class, new TpchSplitManager(connectorId, nodeManager));
        builder.put(ConnectorDataStreamProvider.class, new TpchDataStreamProvider(tpchBlocksProvider));
        builder.put(ConnectorHandleResolver.class, HANDLE_RESOLVER);

        return new StaticConnector(builder.build());
    }
}
