package com.facebook.presto.split;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class ImportClientManager
{
    private final ConcurrentMap<String, ImportClientFactory> clientFactories = new ConcurrentHashMap<>();

    @Inject
    public ImportClientManager(Map<String, ImportClientFactory> clientFactories)
    {
        this.clientFactories.putAll(clientFactories);
    }

    public Map<String, ImportClientFactory> getImportClientFactories()
    {
        return ImmutableMap.copyOf(clientFactories);
    }

    public void addImportClientFactory(String name, ImportClientFactory importClientFactory)
    {
        ImportClientFactory existingClient = clientFactories.putIfAbsent(name, importClientFactory);
        checkState(existingClient == null, "A client factory named %s already exists", name);
    }

    public ImportClient getClient(String clientName)
    {
        ImportClientFactory factory = clientFactories.get(clientName);
        checkArgument(factory != null, "Unknown source '%s'", clientName);
        ImportClient client = factory.createClient(clientName);
        checkArgument(client != null, "Failed to create client %s", clientName);
        return client;
    }
}
