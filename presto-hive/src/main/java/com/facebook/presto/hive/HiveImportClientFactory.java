/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.inject.Inject;

public class HiveImportClientFactory
        implements ImportClientFactory
{
    private static final String HIVE_SOURCE_NAME = "hive";

    private final DiscoveryLocatedHiveCluster discoveryLocatedHiveCluster;
    private final HiveClientFactory hiveClientFactory;

    @Inject
    public HiveImportClientFactory(DiscoveryLocatedHiveCluster discoveryLocatedHiveCluster, HiveClientFactory hiveClientFactory)
    {
        this.discoveryLocatedHiveCluster = discoveryLocatedHiveCluster;
        this.hiveClientFactory = hiveClientFactory;
    }

    @Override
    public ImportClient createClient(String sourceName)
    {
        if (!HIVE_SOURCE_NAME.equals(sourceName)) {
            return null;
        }

        return hiveClientFactory.get(discoveryLocatedHiveCluster);
    }
}
