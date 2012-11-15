package com.facebook.presto.split;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.spi.ImportClient;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportClientFactory
{
    private final Map<String, ImportClient> importClientMap;

    @Inject
    public ImportClientFactory(HiveClient hiveClient)
    {
        checkNotNull(hiveClient, "hiveClient is null");

        importClientMap = ImmutableMap.<String, ImportClient>builder()
                .put("hive", hiveClient)
                .build();
    }

    public ImportClient getClient(String sourceName)
    {
        checkNotNull(sourceName, "sourceName is null");

        ImportClient importClient = importClientMap.get(sourceName);
        checkArgument(importClient != null, "source does not exist: %s", sourceName);

        return importClient;
    }
}
