package com.facebook.presto.split;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Set;

public class ImportClientManager
{
    private final Set<ImportClientFactory> clientFactories;

    @Inject
    public ImportClientManager(Set<ImportClientFactory> clientFactories)
    {
        this.clientFactories = ImmutableSet.copyOf(clientFactories);
    }

    public ImportClient getClient(String sourceName)
    {
        for (ImportClientFactory clientFactory : clientFactories) {
            ImportClient client = clientFactory.createClient(sourceName);
            if (client != null) {
                return client;
            }
        }
        throw new RuntimeException("Unknown source '" + sourceName + "'");
    }
}
