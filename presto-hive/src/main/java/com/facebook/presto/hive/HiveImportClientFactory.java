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
    private final HiveCluster hiveCluster;
    private final HiveClientFactory hiveClientFactory;

    @Inject
    public HiveImportClientFactory(HiveCluster hiveCluster, HiveClientFactory hiveClientFactory)
    {
        this.hiveCluster = hiveCluster;
        this.hiveClientFactory = hiveClientFactory;
    }

    @Override
    public ImportClient createClient(String clientId)
    {
        return hiveClientFactory.get(clientId, hiveCluster);
    }
}
