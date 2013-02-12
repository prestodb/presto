/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.cli;

import com.facebook.presto.metadata.CatalogRegistry;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.thrift.PrestoThriftClientConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class CatalogRegistryModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    @Singleton
    public CatalogRegistry getCatalogRegistry(PrestoThriftClientConfig config)
    {
        CatalogRegistry registry = new CatalogRegistry();

        // TODO this is a hack: figure out way for service providers to register these themselves
        registry.addExactType("default", DataSourceType.NATIVE);
        registry.addPrefixType("hive_", DataSourceType.IMPORT);

        for (String name : config.getServiceAddresses().keys()) {
            registry.addExactType(name, DataSourceType.IMPORT);
        }

        return registry;
    }
}
