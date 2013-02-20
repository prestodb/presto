package com.facebook.presto.split;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.HashSet;
import java.util.Set;

public class ImportClientManager
{
    private final Set<ImportClientFactory> clientFactories = new HashSet<>();

    @Inject
    public ImportClientManager(Set<ImportClientFactory> clientFactories)
    {
        this.clientFactories.addAll(clientFactories);
    }

    public synchronized Set<ImportClientFactory> getImportClientFactories()
    {
        return ImmutableSet.copyOf(clientFactories);
    }

    public synchronized void addImportClientFactory(ImportClientFactory importClientFactory)
    {
        clientFactories.add(importClientFactory);
    }

    public boolean hasCatalog(String catalogName)
    {
        for (ImportClientFactory clientFactory : clientFactories) {
            if (clientFactory.hasCatalog(catalogName)) {
                return true;
            }
        }
        return false;
    }

    public ImportClient getClient(String sourceName)
    {
        for (ImportClientFactory clientFactory : getImportClientFactories()) {
            ImportClient client = clientFactory.createClient(sourceName);
            if (client != null) {
                return client;
            }
        }
        throw new RuntimeException("Unknown source '" + sourceName + "'");
    }
}
