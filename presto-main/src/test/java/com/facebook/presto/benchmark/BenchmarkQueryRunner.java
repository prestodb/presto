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
import com.facebook.presto.metadata.DatabaseLocalStorageManager;
import com.facebook.presto.metadata.DatabaseLocalStorageManagerConfig;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NativeRecordSinkProvider;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.NativeSplitManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.util.LocalQueryRunner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import java.io.File;
import java.util.concurrent.ExecutorService;

public final class BenchmarkQueryRunner
{
    private BenchmarkQueryRunner()
    {
    }

    public static LocalQueryRunner createLocalQueryRunner(ExecutorService executor)
    {
        Session session = new Session("user", "test", "default", "default", null, null);
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session, executor);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // add native
        MetadataManager metadata = localQueryRunner.getMetadata();
        NativeConnectorFactory nativeConnectorFactory = createNativeConnectorFactory(nodeManager, metadata);
        localQueryRunner.createCatalog("default", nativeConnectorFactory, ImmutableMap.<String, String>of());

        if (!metadata.getTableHandle(new QualifiedTableName("default", "default", "orders")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        }
        if (!metadata.getTableHandle(new QualifiedTableName("default", "default", "lineitem")).isPresent()) {
            localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        }
        return localQueryRunner;
    }

    private static NativeConnectorFactory createNativeConnectorFactory(NodeManager nodeManager, Metadata metadata)
    {
        try {
            File dataDir = new File(System.getProperty("tpchCacheDir", "/tmp/tpch_data_cache"));
            File databaseDir = new File(dataDir, "db");

            IDBI localStorageManagerDbi = createDataSource(databaseDir, "StorageManager");

            DatabaseLocalStorageManagerConfig storageManagerConfig = new DatabaseLocalStorageManagerConfig()
                    .setCompressed(false)
                    .setDataDirectory(new File(dataDir, "data"));
            LocalStorageManager localStorageManager = new DatabaseLocalStorageManager(localStorageManagerDbi, storageManagerConfig);

            NativeDataStreamProvider nativeDataStreamProvider = new NativeDataStreamProvider(localStorageManager);

            NativeRecordSinkProvider nativeRecordSinkProvider = new NativeRecordSinkProvider(localStorageManager, nodeManager.getCurrentNode().getNodeIdentifier());

            IDBI metadataDbi = createDataSource(databaseDir, "Metastore");
            NativeConnectorFactory nativeConnectorFactory = new NativeConnectorFactory(nativeDataStreamProvider, nativeRecordSinkProvider);
            nativeConnectorFactory.setDbi(metadataDbi);
            DatabaseShardManager shardManager = new DatabaseShardManager(metadataDbi);
            nativeConnectorFactory.setShardManager(shardManager);
            nativeConnectorFactory.setSplitManager(new NativeSplitManager(nodeManager, shardManager, metadata));

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

    private static IDBI createDataSource(File databaseDir, String name)
            throws Exception
    {
        H2EmbeddedDataSourceConfig dataSourceConfig = new H2EmbeddedDataSourceConfig();
        dataSourceConfig.setFilename(new File(databaseDir, name).getAbsolutePath());
        H2EmbeddedDataSource dataSource = new H2EmbeddedDataSource(dataSourceConfig);
        return new DBI(dataSource);
    }
}
