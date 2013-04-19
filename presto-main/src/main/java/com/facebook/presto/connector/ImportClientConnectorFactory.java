package com.facebook.presto.connector;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.metadata.ImportHandleResolver;
import com.facebook.presto.metadata.ImportMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ImportClientManager;
import com.facebook.presto.split.ImportDataStreamProvider;
import com.facebook.presto.split.ImportSplitManager;
import com.google.common.collect.ImmutableClassToInstanceMap;

import java.util.Map;

public class ImportClientConnectorFactory
        implements ConnectorFactory
{
    private final ImportClientManager importClientManager;

    public ImportClientConnectorFactory(ImportClientManager importClientManager)
    {
        this.importClientManager = importClientManager;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        // assume connectorId is client name
        ImportClient client = importClientManager.getClient(connectorId);

        ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
        builder.put(ConnectorMetadata.class, new ImportMetadata(client));
        builder.put(ConnectorSplitManager.class, new ImportSplitManager(client));
        builder.put(ConnectorDataStreamProvider.class, new ImportDataStreamProvider(client));
        builder.put(ConnectorHandleResolver.class, new ImportHandleResolver(client));

        return new StaticConnector(builder.build());
    }
}
