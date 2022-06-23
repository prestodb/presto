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

package com.facebook.presto.hive;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreStats;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_DATABASE;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_TABLE_WITH_CONSTRAINTS;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHiveTableConstraints
{
    private MockHiveMetastoreClient mockClient;
    private SemiTransactionalHiveMetastore metastore;
    private ThriftHiveMetastoreStats stats;

    @BeforeMethod
    public void setUp()
    {
        HiveClientConfig config = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        PartitionMutator hivePartitionMutator = new HivePartitionMutator();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        ThriftHiveMetastore thriftHiveMetastore = new ThriftHiveMetastore(mockHiveCluster, metastoreClientConfig, hdfsEnvironment);
        metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                new BridgingHiveMetastore(thriftHiveMetastore, hivePartitionMutator),
                executor,
                false,
                false,
                true,
                DEFAULT_COLUMN_CONVERTER_PROVIDER);
        stats = thriftHiveMetastore.getStats();
    }

    @Test
    public void testTableConstraints()
    {
        List<TableConstraint<String>> expectedConstraints = ImmutableList.of(
                new PrimaryKeyConstraint<>("", ImmutableSet.of("c1"), true, true),
                new UniqueConstraint<>("", ImmutableSet.of("c2"), true, true));

        List<TableConstraint<String>> tableConstraints = metastore.getTableConstraints(METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(tableConstraints, expectedConstraints);

        ConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setMaxBucketsForGroupedExecution(100),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig())
                        .getSessionProperties());

        metastore.dropTable(new HdfsContext(session, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS, "/some/path", false), TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        tableConstraints = metastore.getTableConstraints(METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(tableConstraints, ImmutableList.of());
    }

    public static class MockHiveCluster
            implements HiveCluster
    {
        private final MockHiveMetastoreClient client;

        public MockHiveCluster(MockHiveMetastoreClient client)
        {
            this.client = client;
        }

        @Override
        public HiveMetastoreClient createMetastoreClient(Optional<String> token)
        {
            return client;
        }

        public MockHiveMetastoreClient createPartitionVersionSupportedMetastoreClient()
        {
            return client;
        }
    }
}
