/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.metadata.DatabaseMetadata;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.ForStorageManager;
import com.facebook.presto.metadata.HiveImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StorageManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Singleton;

public class ServerMainModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(StaticQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);
        // TODO: provide these metastore connection params via config
        //binder.bind(HiveClient.class).toInstance(new HiveClient("10.38.14.61", 9083));
        binder.bind(HiveClient.class).toInstance(new HiveClient("localhost", 9083));
        binder.bind(StorageManager.class).to(DatabaseStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(HiveImportManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(DatabaseMetadata.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForStorageManager
    public IDBI createStorageManagerDBI()
    {
        // TODO: configuration
        DBI dbi = new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1");
        return dbi;
    }

    @Provides
    @Singleton
    @ForMetadata
    public IDBI createMetadataDBI()
    {
        // TODO: configuration
        DBI dbi = new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1");
        return dbi;
    }
}
