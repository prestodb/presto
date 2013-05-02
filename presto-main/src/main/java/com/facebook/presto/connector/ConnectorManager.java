package com.facebook.presto.connector;

import com.facebook.presto.metadata.CollocatedSplitHandleResolver;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalHandleResolver;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.RemoteSplitHandleResolver;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.RecordSetDataStreamProvider;
import com.facebook.presto.split.SplitManager;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ConnectorManager
{
    private final MetadataManager metadataManager;
    private final SplitManager splitManager;
    private final DataStreamManager dataStreamManager;
    private final HandleResolver handleResolver;

    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Connector> connectors = new ConcurrentHashMap<>();

    @Inject
    public ConnectorManager(MetadataManager metadataManager,
            SplitManager splitManager,
            DataStreamManager dataStreamManager,
            HandleResolver handleResolver,
            Map<String, ConnectorFactory> connectorFactories)
    {
        this.metadataManager = metadataManager;
        this.splitManager = splitManager;
        this.dataStreamManager = dataStreamManager;
        this.handleResolver = handleResolver;
        this.connectorFactories.putAll(connectorFactories);

        // for now just hard code the handle resolvers
        handleResolver.addHandleResolver("internal", new InternalHandleResolver());
        handleResolver.addHandleResolver("remote", new RemoteSplitHandleResolver());
        handleResolver.addHandleResolver("collocated", new CollocatedSplitHandleResolver());
    }

    public void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(connectorFactory.getName(), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector %s is already registered", connectorFactory.getName());
    }

    public synchronized void createConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(connectorName, "connectorName is null");
        checkNotNull(properties, "properties is null");

        // for now connectorId == catalogName
        String connectorId = catalogName;
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        Preconditions.checkArgument(connectorFactory != null, "No factory for connector %s", connectorName);

        Connector connector = connectorFactory.create(connectorId, properties);
        connectors.put(connectorName, connector);

        ConnectorMetadata connectorMetadata = connector.getService(ConnectorMetadata.class);
        checkState(connectorMetadata != null, "Connector %s can not provide metadata", connectorId);

        ConnectorSplitManager connectorSplitManager = connector.getService(ConnectorSplitManager.class);
        checkState(connectorSplitManager != null, "Connector %s does not have a split manager", connectorId);

        ConnectorDataStreamProvider connectorDataStreamProvider = connector.getService(ConnectorDataStreamProvider.class);
        if (connectorDataStreamProvider == null) {
            ConnectorRecordSetProvider connectorRecordSetProvider = connector.getService(ConnectorRecordSetProvider.class);
            checkState(connectorRecordSetProvider != null, "Connector %s does not have a data stream provider", connectorId);
            connectorDataStreamProvider = new RecordSetDataStreamProvider(connectorRecordSetProvider);
        }

        ConnectorHandleResolver connectorHandleResolver = connector.getService(ConnectorHandleResolver.class);
        checkState(connectorDataStreamProvider != null, "Connector %s does not have a handle resolver", connectorId);

        metadataManager.addConnectorMetadata(catalogName, connectorMetadata);
        handleResolver.addHandleResolver(connectorId, connectorHandleResolver);
        splitManager.addConnectorSplitManager(connectorSplitManager);
        dataStreamManager.addConnectorDataStreamProvider(connectorDataStreamProvider);
    }
}
