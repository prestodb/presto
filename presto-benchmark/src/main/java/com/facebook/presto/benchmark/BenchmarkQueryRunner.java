/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.benchmark;

import com.facebook.presto.connector.NativeConnectorFactory;
import com.facebook.presto.metadata.ColumnMetadataMapper;
import com.facebook.presto.metadata.DatabaseLocalStorageManager;
import com.facebook.presto.metadata.DatabaseLocalStorageManagerConfig;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NativeConnectorId;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.NativeRecordSinkProvider;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableColumnMapper;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.NativePartitionKey;
import com.facebook.presto.split.NativeSplitManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.testing.SampledTpchConnectorFactory;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.testing.TestingBlockEncodingManager.createTestingBlockEncodingManager;

public final class BenchmarkQueryRunner
{
    private BenchmarkQueryRunner()
    {
    }

    public static LocalQueryRunner createLocalSampledQueryRunner(ExecutorService executor)
    {
        ConnectorSession session = new ConnectorSession("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, null, null);
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session, executor);

        // add sampled tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch_sampled", new SampledTpchConnectorFactory(nodeManager, 1, 2), ImmutableMap.<String, String>of());

        // add native
        NativeConnectorFactory nativeConnectorFactory = createNativeConnectorFactory(
                nodeManager,
                localQueryRunner.getTypeManager(),
                System.getProperty("tpchSampledCacheDir", "/tmp/presto_tpch/sampled_data_cache"));
        localQueryRunner.createCatalog("default", nativeConnectorFactory, ImmutableMap.<String, String>of());

        MetadataManager metadata = localQueryRunner.getMetadata();
        if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "orders")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch_sampled.sf1.orders");
        }
        if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "lineitem")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch_sampled.sf1.lineitem");
        }
        return localQueryRunner;
    }

    public static LocalQueryRunner createLocalQueryRunner(ExecutorService executor)
    {
        ConnectorSession session = new ConnectorSession("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, null, null);
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session, executor);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // add native
        NativeConnectorFactory nativeConnectorFactory = createNativeConnectorFactory(
                nodeManager,
                localQueryRunner.getTypeManager(),
                System.getProperty("tpchCacheDir", "/tmp/presto_tpch/data_cache"));
        localQueryRunner.createCatalog("default", nativeConnectorFactory, ImmutableMap.<String, String>of());

        MetadataManager metadata = localQueryRunner.getMetadata();
        if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "orders")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        }
        if (!metadata.getTableHandle(session, new QualifiedTableName("default", "default", "lineitem")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        }
        return localQueryRunner;
    }

    private static NativeConnectorFactory createNativeConnectorFactory(NodeManager nodeManager, TypeRegistry typeRegistry, String cacheDir)
    {
        try {
            File dataDir = new File(cacheDir);
            File databaseDir = new File(dataDir, "db");

            IDBI localStorageManagerDbi = createDataSource(typeRegistry, databaseDir, "StorageManager");

            DatabaseLocalStorageManagerConfig storageManagerConfig = new DatabaseLocalStorageManagerConfig()
                    .setCompressed(false)
                    .setDataDirectory(new File(dataDir, "data"));
            LocalStorageManager localStorageManager = new DatabaseLocalStorageManager(localStorageManagerDbi, createTestingBlockEncodingManager(), storageManagerConfig);

            NativeDataStreamProvider nativeDataStreamProvider = new NativeDataStreamProvider(localStorageManager);

            NativeRecordSinkProvider nativeRecordSinkProvider = new NativeRecordSinkProvider(localStorageManager, nodeManager.getCurrentNode().getNodeIdentifier());

            IDBI metadataDbi = createDataSource(typeRegistry, databaseDir, "Metastore");
            DatabaseShardManager shardManager = new DatabaseShardManager(metadataDbi);
            NativeMetadata nativeMetadata = new NativeMetadata(new NativeConnectorId("default"), metadataDbi, shardManager);
            NativeSplitManager nativeSplitManager = new NativeSplitManager(nodeManager, shardManager, nativeMetadata);
            NativeConnectorFactory nativeConnectorFactory = new NativeConnectorFactory(nativeMetadata, nativeSplitManager, nativeDataStreamProvider, nativeRecordSinkProvider);

            return nativeConnectorFactory;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static IDBI createDataSource(TypeRegistry typeRegistry, File databaseDir, String name)
            throws Exception
    {
        H2EmbeddedDataSourceConfig dataSourceConfig = new H2EmbeddedDataSourceConfig();
        dataSourceConfig.setFilename(new File(databaseDir, name).getAbsolutePath());
        H2EmbeddedDataSource dataSource = new H2EmbeddedDataSource(dataSourceConfig);
        DBI dbi = new DBI(dataSource);

        dbi.registerMapper(new TableColumnMapper(typeRegistry));
        dbi.registerMapper(new ColumnMetadataMapper(typeRegistry));
        dbi.registerMapper(new NativePartitionKey.Mapper(typeRegistry));
        return dbi;
    }
}
