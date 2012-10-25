/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.metadata.HiveImportManager;
import com.facebook.presto.metadata.StorageManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

public class ServerMainModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(StaticQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(UncompressedBlockMapper.class).in(Scopes.SINGLETON);
        binder.bind(UncompressedBlocksMapper.class).in(Scopes.SINGLETON);
        // TODO: provide these metastore connection params via config
        //binder.bind(HiveClient.class).toInstance(new HiveClient("10.38.14.61", 9083));
        binder.bind(HiveClient.class).toInstance(new HiveClient("localhost", 9083));
        binder.bind(StorageManager.class).to(DatabaseStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(HiveImportManager.class).in(Scopes.SINGLETON);
        // TODO: use a better way to bind this (e.g. provider)
        binder.bind(IDBI.class).toInstance(new DBI("jdbc:h2:file:var/presto-data/db/db"));
    }
}
