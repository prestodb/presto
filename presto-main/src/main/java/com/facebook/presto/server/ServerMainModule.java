/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.importer.ForImportManager;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.importer.NodeWorkerQueue;
import com.facebook.presto.importer.ShardImporter;
import com.facebook.presto.metadata.DatabaseMetadata;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.ForShardManager;
import com.facebook.presto.metadata.ForStorageManager;
import com.facebook.presto.metadata.ImportMetadata;
import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.metadata.LegacyStorageManagerFacade;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.split.ImportDataStreamProvider;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.SplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Singleton;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class ServerMainModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(StaticQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);

        binder.bind(StorageManager.class).to(DatabaseStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(LegacyStorageManager.class).to(LegacyStorageManagerFacade.class).in(Scopes.SINGLETON);
        binder.bind(DataStreamProvider.class).to(DataStreamManager.class).in(Scopes.SINGLETON);
        binder.bind(NativeDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(ImportDataStreamProvider.class).in(Scopes.SINGLETON);

        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseMetadata.class).in(Scopes.SINGLETON);

        binder.bind(ImportClient.class).to(HiveClient.class).in(Scopes.SINGLETON);
        binder.bind(ImportClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(ImportMetadata.class).in(Scopes.SINGLETON);

        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(QueryFragmentRequest.class);

        discoveryBinder(binder).bindSelector("presto");
        binder.bind(NodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeWorkerQueue.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ImportManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("importer", ForImportManager.class).withFilter(NodeIdUserAgentRequestFilter.class);
        binder.bind(ShardImporter.class).in(Scopes.SINGLETON);
        binder.bind(ShardResource.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(ShardImport.class);

        discoveryBinder(binder).bindHttpAnnouncement("presto");
    }

    @Provides
    @Singleton
    public HiveClient createHiveClient()
    {
        // TODO: configuration
        return new HiveClient("localhost", 9083);
    }

    @Provides
    @Singleton
    @ForStorageManager
    public IDBI createStorageManagerDBI()
    {
        // TODO: configuration
        return new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1;MVCC=TRUE");
    }

    @Provides
    @Singleton
    @ForMetadata
    public IDBI createMetadataDBI()
    {
        // TODO: configuration
        return new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1;MVCC=TRUE");
    }

    @Provides
    @Singleton
    @ForShardManager
    public IDBI createShardManagerDBI()
    {
        // TODO: configuration
        return new DBI("jdbc:h2:file:var/presto-data/db/ShardManager;DB_CLOSE_DELAY=-1;MVCC=TRUE");
    }
}
