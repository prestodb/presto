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
    private static final String HIVE_CATALOG_NAME = "hive";

    private final HiveCluster hiveCluster;
    private final HiveClientFactory hiveClientFactory;

    @Inject
    public HiveImportClientFactory(HiveCluster hiveCluster, HiveClientFactory hiveClientFactory)
    {
        this.hiveCluster = hiveCluster;
        this.hiveClientFactory = hiveClientFactory;
    }

    @Override
    public boolean hasCatalog(String catalogName)
    {
        return HIVE_CATALOG_NAME.equals(catalogName);
    }

    @Override
    public ImportClient createClient(String catalogName)
    {
        if (!HIVE_CATALOG_NAME.equals(catalogName)) {
            return null;
        }

        return hiveClientFactory.get(hiveCluster);
    }
}
