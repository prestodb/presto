package com.facebook.presto.connector;

import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.ImportHandleResolver;
import com.facebook.presto.metadata.ImportMetadata;
import com.facebook.presto.metadata.InternalHandleResolver;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NativeHandleResolver;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.split.ImportClientManager;
import com.facebook.presto.split.ImportSplitManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.google.inject.Inject;

public class ConnectorManager
{
    private final MetadataManager metadataManager;
    private final SplitManager splitManager;
    private final ImportClientManager importClientManager;
    private final HandleResolver handleResolver;

    @Inject
    public ConnectorManager(MetadataManager metadataManager, SplitManager splitManager, ImportClientManager importClientManager, HandleResolver handleResolver)
    {
        this.metadataManager = metadataManager;
        this.splitManager = splitManager;
        this.importClientManager = importClientManager;
        this.handleResolver = handleResolver;

        // for not just hard code the handle resolvers
        handleResolver.addHandleResolver("native", new NativeHandleResolver());
        handleResolver.addHandleResolver("tpch", new TpchHandleResolver());
        handleResolver.addHandleResolver("internal", new InternalHandleResolver());
        handleResolver.addHandleResolver("import", new ImportHandleResolver());
    }

    public void initialize()
    {
        // list is lame, but just register all plugins into a catalog for now
        for (String connectorName : importClientManager.getImportClientFactories().keySet()) {
            createConnection(connectorName, connectorName);
        }
    }

    public void createConnection(String catalogName, String connectorName)
    {
        ImportClient client = importClientManager.getClient(catalogName);
        ConnectorMetadata connectorMetadata = new ImportMetadata(catalogName, client);
        metadataManager.addConnectorMetadata(catalogName, connectorMetadata);
        splitManager.addConnectorSplitManager(new ImportSplitManager(catalogName, client));
    }
}
