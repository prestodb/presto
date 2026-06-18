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
package com.facebook.presto.hive.metastore;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.MockHiveMetastore;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreStats;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType.ALL;
import static com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType.PARTITION;
import static com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType.PARTITION_STATISTICS;
import static com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType.TABLE;
import static com.facebook.presto.hive.metastore.NoopMetastoreCacheStats.NOOP_METASTORE_CACHE_STATS;
import static com.facebook.presto.hive.metastore.Partition.Builder;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.BAD_DATABASE;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.PARTITION_VERSION;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_DATABASE;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_METASTORE_CONTEXT;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION1;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION2;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION_NAME_WITHOUT_VERSION1;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION_NAME_WITHOUT_VERSION2;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION_NAME_WITH_VERSION1;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION_NAME_WITH_VERSION2;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION_VALUES1;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION_VALUES2;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_ROLES;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_TABLE;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_TABLE_WITH_CONSTRAINTS;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.function.UnaryOperator.identity;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestInMemoryCachingHiveMetastore
{
    private static final ImmutableList<PartitionNameWithVersion> EXPECTED_PARTITIONS = ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2);

    private MockHiveMetastoreClient mockClient;
    private InMemoryCachingHiveMetastore metastoreWithAllCachesEnabled;
    private InMemoryCachingHiveMetastore metastoreWithSelectiveCachesEnabled;
    private ThriftHiveMetastoreStats stats;

    @BeforeMethod
    public void setUp()
    {
        mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        // Configure Metastore Cache
        metastoreClientConfig.setDefaultMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES));
        metastoreClientConfig.setDefaultMetastoreCacheRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        metastoreClientConfig.setMetastoreCacheMaximumSize(1000);
        metastoreClientConfig.setEnabledCaches(ALL.name());

        ThriftHiveMetastore thriftHiveMetastore = new ThriftHiveMetastore(mockHiveCluster, metastoreClientConfig, HDFS_ENVIRONMENT);
        PartitionMutator hivePartitionMutator = new HivePartitionMutator();
        metastoreWithAllCachesEnabled = new InMemoryCachingHiveMetastore(
                new BridgingHiveMetastore(thriftHiveMetastore, hivePartitionMutator),
                executor,
                false,
                1000,
                false,
                0.0,
                metastoreClientConfig.getPartitionCacheColumnCountLimit(),
                NOOP_METASTORE_CACHE_STATS,
                new MetastoreCacheSpecProvider(metastoreClientConfig));

        MetastoreClientConfig metastoreClientConfigWithSelectiveCaching = new MetastoreClientConfig();
        // Configure Metastore Cache
        metastoreClientConfigWithSelectiveCaching.setDefaultMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES));
        metastoreClientConfigWithSelectiveCaching.setDefaultMetastoreCacheRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        metastoreClientConfigWithSelectiveCaching.setMetastoreCacheMaximumSize(1000);
        metastoreClientConfigWithSelectiveCaching.setDisabledCaches(TABLE.name());

        ThriftHiveMetastore thriftHiveMetastoreWithSelectiveCaching = new ThriftHiveMetastore(mockHiveCluster, metastoreClientConfigWithSelectiveCaching, HDFS_ENVIRONMENT);
        metastoreWithSelectiveCachesEnabled = new InMemoryCachingHiveMetastore(
                new BridgingHiveMetastore(thriftHiveMetastoreWithSelectiveCaching, hivePartitionMutator),
                executor,
                false,
                1000,
                false,
                0.0,
                metastoreClientConfigWithSelectiveCaching.getPartitionCacheColumnCountLimit(),
                NOOP_METASTORE_CACHE_STATS,
                new MetastoreCacheSpecProvider(metastoreClientConfigWithSelectiveCaching));

        stats = thriftHiveMetastore.getStats();
    }

    @Test
    public void testGetAllDatabases()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastoreWithAllCachesEnabled.getAllDatabases(TEST_METASTORE_CONTEXT), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastoreWithAllCachesEnabled.getAllDatabases(TEST_METASTORE_CONTEXT), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithAllCachesEnabled.invalidateAll();

        assertEquals(metastoreWithAllCachesEnabled.getAllDatabases(TEST_METASTORE_CONTEXT), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 2);

        // Test invalidate a specific database
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE);
        assertEquals(metastoreWithAllCachesEnabled.getAllDatabases(TEST_METASTORE_CONTEXT), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 3);
    }

    @Test
    public void testGetAllTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithAllCachesEnabled.invalidateAll();

        assertEquals(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 2);

        // Test invalidate a specific database which will also invalidate all table caches mapped to that database
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE);
        assertEquals(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 3);
        assertEquals(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 3);

        // Test invalidate a specific database.table which also invalidates the tablesNamesCache for that database
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE);
        assertEquals(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 4);
    }

    @Test
    public void testGetAllTableWithSelectiveCaching()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastoreWithSelectiveCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastoreWithSelectiveCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithSelectiveCachesEnabled.invalidateAll();

        assertEquals(metastoreWithSelectiveCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 2);
    }

    public void testInvalidDbGetAllTAbles()
    {
        assertFalse(metastoreWithAllCachesEnabled.getAllTables(TEST_METASTORE_CONTEXT, BAD_DATABASE).isPresent());
    }

    @Test
    public void testGetTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithAllCachesEnabled.invalidateAll();

        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);

        // Test invalidate a specific database which will also invalidate all table caches mapped to that database
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE);
        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 3);
        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 3);

        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 4);

        // Test invalidate a specific table
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE);
        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS));
        assertEquals(mockClient.getAccessCount(), 4);
        assertNotNull(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 5);
    }

    @Test
    public void testGetTableWithSelectiveCaching()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertNotNull(metastoreWithSelectiveCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertNotNull(metastoreWithSelectiveCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
    }

    public void testInvalidDbGetTable()
    {
        assertFalse(metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, BAD_DATABASE, TEST_TABLE).isPresent());

        assertEquals(stats.getGetTable().getThriftExceptions().getTotalCount(), 0);
        assertEquals(stats.getGetTable().getTotalFailures().getTotalCount(), 0);
        assertNotNull(stats.getGetTable().getTime());
    }

    @Test
    public void testGetPartitionNames()
    {
        ImmutableList<PartitionNameWithVersion> expectedPartitions = ImmutableList.of(TEST_PARTITION_NAME_WITHOUT_VERSION1, TEST_PARTITION_NAME_WITHOUT_VERSION2);
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithAllCachesEnabled.invalidateAll();

        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);

        // Test invalidate the database which will also invalidate all linked table and partition caches
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 3);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 3);

        // Test invalidate a specific table which will also invalidate all linked partition caches
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 4);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 4);

        // Test invalidate a specific partition
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of("key"), ImmutableList.of("testpartition1"));
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 5);
    }

    @Test
    public void testInvalidInvalidateCache()
    {
        // Test invalidate cache with null/empty database name
        assertThatThrownBy(() -> metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("databaseName cannot be null or empty");

        // Test invalidate cache with null/empty table name
        assertThatThrownBy(() -> metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("tableName cannot be null or empty");

        // Test invalidate cache with invalid/empty partition columns list
        assertThatThrownBy(() -> metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(), ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitionColumnNames cannot be null or empty");

        // Test invalidate cache with invalid/empty partition values list
        assertThatThrownBy(() -> metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of("key"), ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitionValues cannot be null or empty");

        // Test invalidate cache with mismatched partition columns and values list
        assertThatThrownBy(() -> metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of("key1", "key2"), ImmutableList.of("testpartition1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitionColumnNames and partitionValues should be of same length");
    }

    @Test
    public void testInvalidGetPartitionNames()
    {
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNames(TEST_METASTORE_CONTEXT, BAD_DATABASE, TEST_TABLE).get(), ImmutableList.of());
    }

    @Test
    public void testGetPartitionNamesByParts()
    {
        ImmutableList<PartitionNameWithVersion> expectedPartitions = ImmutableList.of(TEST_PARTITION_NAME_WITHOUT_VERSION1, TEST_PARTITION_NAME_WITHOUT_VERSION2);

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithAllCachesEnabled.invalidateAll();

        assertEquals(metastoreWithAllCachesEnabled.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
    }

    private static class MockPartitionMutator
            implements PartitionMutator
    {
        private final Function<Long, Long> versionUpdater;
        private long version = PARTITION_VERSION;

        public MockPartitionMutator(Function<Long, Long> versionUpdater)
        {
            this.versionUpdater = versionUpdater;
        }

        @Override
        public void mutate(Builder builder, org.apache.hadoop.hive.metastore.api.Partition partition)
        {
            version = versionUpdater.apply(version);
            builder.setPartitionVersion(version);
        }
    }

    @Test
    public void testCachingWithPartitionVersioning()
    {
        MockHiveMetastoreClient mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("partition-versioning-test-%s")));
        MockHiveMetastore mockHiveMetastore = new MockHiveMetastore(mockHiveCluster);
        PartitionMutator mockPartitionMutator = new MockPartitionMutator(identity());
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        // Configure Metastore Cache
        metastoreClientConfig.setDefaultMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES));
        metastoreClientConfig.setDefaultMetastoreCacheRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        metastoreClientConfig.setMetastoreCacheMaximumSize(1000);
        metastoreClientConfig.setEnabledCaches(String.join(",", PARTITION.name(), PARTITION_STATISTICS.name()));

        InMemoryCachingHiveMetastore partitionCachingEnabledmetastore = new InMemoryCachingHiveMetastore(
                new BridgingHiveMetastore(mockHiveMetastore, mockPartitionMutator),
                executor,
                false,
                1000,
                true,
                0.0,
                10_000,
                NOOP_METASTORE_CACHE_STATS,
                new MetastoreCacheSpecProvider(metastoreClientConfig));

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
        // Assert that we did not hit cache
        assertEquals(mockClient.getAccessCount(), 2);

        // Select all of the available partitions and load them into the cache
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        // Now if we fetch any or both of them, they should hit the cache
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1)).size(), 1);
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION2)).size(), 1);
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        // This call should NOT invalidate the partition cache because partition version is same as before
        assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
        assertEquals(mockClient.getAccessCount(), 4);

        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        // Assert that its a cache hit
        assertEquals(mockClient.getAccessCount(), 4);

        Function<Long, Long> versionIncrement = version -> version + 1;
        Function<Long, Long> versionDecrement = version -> version + 1;
        assertInvalidateCache(new MockPartitionMutator(versionIncrement), versionIncrement);
        assertInvalidateCache(new MockPartitionMutator(versionDecrement), versionDecrement);
    }

    private void assertInvalidateCache(MockPartitionMutator partitionMutator, Function<Long, Long> versionMutator)
    {
        MockHiveMetastoreClient mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("partition-versioning-test-%s")));
        MockHiveMetastore mockHiveMetastore = new MockHiveMetastore(mockHiveCluster);
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        // Configure Metastore Cache
        metastoreClientConfig.setDefaultMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES));
        metastoreClientConfig.setDefaultMetastoreCacheRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        metastoreClientConfig.setMetastoreCacheMaximumSize(1000);
        metastoreClientConfig.setEnabledCaches(String.join(",", PARTITION.name(), PARTITION_STATISTICS.name()));

        InMemoryCachingHiveMetastore partitionCachingEnabledmetastore = new InMemoryCachingHiveMetastore(
                new BridgingHiveMetastore(mockHiveMetastore, partitionMutator),
                executor,
                false,
                1000,
                true,
                0.0,
                10_000,
                NOOP_METASTORE_CACHE_STATS,
                new MetastoreCacheSpecProvider(metastoreClientConfig));

        int clientAccessCount = 0;
        for (int i = 0; i < 100; i++) {
            assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
            assertEquals(mockClient.getAccessCount(), ++clientAccessCount);
            PartitionNameWithVersion partitionNameWithVersion1 = new PartitionNameWithVersion(TEST_PARTITION1, Optional.of(versionMutator.apply(PARTITION_VERSION + (long) (i - 1))));
            PartitionNameWithVersion partitionNameWithVersion2 = new PartitionNameWithVersion(TEST_PARTITION2, Optional.of(versionMutator.apply(PARTITION_VERSION + (long) (i - 1))));
            assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(partitionNameWithVersion1, partitionNameWithVersion2)).size(), 2);
            // Assert that we did not hit cache
            assertEquals(mockClient.getAccessCount(), ++clientAccessCount);
        }
    }

    public void testInvalidGetPartitionNamesByParts()
    {
        assertTrue(metastoreWithAllCachesEnabled.getPartitionNamesByFilter(TEST_METASTORE_CONTEXT, BAD_DATABASE, TEST_TABLE, ImmutableMap.of()).isEmpty());
    }

    @Test
    public void testPartitionCacheValidation()
    {
        MockHiveMetastoreClient mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("partition-versioning-test-%s")));
        MockHiveMetastore mockHiveMetastore = new MockHiveMetastore(mockHiveCluster);
        PartitionMutator mockPartitionMutator = new MockPartitionMutator(identity());
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        // Configure Metastore Cache
        metastoreClientConfig.setDefaultMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES));
        metastoreClientConfig.setDefaultMetastoreCacheRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        metastoreClientConfig.setMetastoreCacheMaximumSize(1000);
        metastoreClientConfig.setEnabledCaches(String.join(",", PARTITION.name(), PARTITION_STATISTICS.name()));

        InMemoryCachingHiveMetastore partitionCacheVerificationEnabledMetastore = new InMemoryCachingHiveMetastore(
                new BridgingHiveMetastore(mockHiveMetastore, mockPartitionMutator),
                executor,
                false,
                1000,
                true,
                100.0,
                10_000,
                NOOP_METASTORE_CACHE_STATS,
                new MetastoreCacheSpecProvider(metastoreClientConfig));

        // Warmup the cache
        partitionCacheVerificationEnabledMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2));

        // Each of the following calls will be a cache hit and verification will be done.
        partitionCacheVerificationEnabledMetastore.getPartition(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, TEST_PARTITION_VALUES1);
        partitionCacheVerificationEnabledMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1));
        partitionCacheVerificationEnabledMetastore.getPartition(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, TEST_PARTITION_VALUES2);
        partitionCacheVerificationEnabledMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1));
    }

    @Test
    public void testPartitionCacheColumnCountLimit()
    {
        MockHiveMetastoreClient mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("partition-versioning-test-%s")));
        MockHiveMetastore mockHiveMetastore = new MockHiveMetastore(mockHiveCluster);
        PartitionMutator mockPartitionMutator = new MockPartitionMutator(identity());
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        // Configure Metastore Cache
        metastoreClientConfig.setDefaultMetastoreCacheTtl(new Duration(5, TimeUnit.MINUTES));
        metastoreClientConfig.setDefaultMetastoreCacheRefreshInterval(new Duration(1, TimeUnit.MINUTES));
        metastoreClientConfig.setMetastoreCacheMaximumSize(1000);
        metastoreClientConfig.setEnabledCaches(String.join(",", PARTITION.name(), PARTITION_STATISTICS.name()));

        InMemoryCachingHiveMetastore partitionCachingEnabledMetastore = new InMemoryCachingHiveMetastore(
                new BridgingHiveMetastore(mockHiveMetastore, mockPartitionMutator),
                executor,
                false,
                1000,
                true,
                0.0,
                // set the cached partition column count limit as 1 for testing purpose
                1,
                NOOP_METASTORE_CACHE_STATS,
                new MetastoreCacheSpecProvider(metastoreClientConfig));

        // Select all of the available partitions. Normally they would have been loaded into the cache. But because of column count limit, they will not be cached
        assertEquals(partitionCachingEnabledMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(partitionCachingEnabledMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1)).size(), 1);
        // Assert that mockClient is used to fetch data since its a cache miss
        assertEquals(mockClient.getAccessCount(), 2);

        assertEquals(partitionCachingEnabledMetastore.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1)).size(), 1);
        // Assert that mockClient is used to fetch data since its a cache miss
        assertEquals(mockClient.getAccessCount(), 3);
    }

    @Test
    public void testGetPartitionsByNames()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        metastoreWithAllCachesEnabled.getTable(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE);
        assertEquals(mockClient.getAccessCount(), 1);

        // Select half of the available partitions and load them into the cache
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1)).size(), 1);
        assertEquals(mockClient.getAccessCount(), 2);

        // Now select all of the partitions
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        // There should be one more access to fetch the remaining partition
        assertEquals(mockClient.getAccessCount(), 3);

        // Now if we fetch any or both of them, they should not hit the client
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1)).size(), 1);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION2)).size(), 1);
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        metastoreWithAllCachesEnabled.invalidateAll();

        // Fetching both should only result in one batched access
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 4);

        // Test invalidate a specific partition
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of("key"), ImmutableList.of("testpartition1"));

        // This should still be a cache hit
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION2)).size(), 1);
        assertEquals(mockClient.getAccessCount(), 4);

        // This should be a cache miss
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1)).size(), 1);
        assertEquals(mockClient.getAccessCount(), 5);

        // This should be a cache hit
        assertEquals(metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 5);
    }

    @Test
    public void testListRoles()
            throws Exception
    {
        assertEquals(mockClient.getAccessCount(), 0);

        assertEquals(metastoreWithAllCachesEnabled.listRoles(TEST_METASTORE_CONTEXT), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(metastoreWithAllCachesEnabled.listRoles(TEST_METASTORE_CONTEXT), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 1);

        metastoreWithAllCachesEnabled.invalidateAll();

        assertEquals(metastoreWithAllCachesEnabled.listRoles(TEST_METASTORE_CONTEXT), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 2);

        metastoreWithAllCachesEnabled.createRole(TEST_METASTORE_CONTEXT, "role", "grantor");

        assertEquals(metastoreWithAllCachesEnabled.listRoles(TEST_METASTORE_CONTEXT), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 3);

        metastoreWithAllCachesEnabled.dropRole(TEST_METASTORE_CONTEXT, "testrole");

        assertEquals(metastoreWithAllCachesEnabled.listRoles(TEST_METASTORE_CONTEXT), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 4);
    }

    public void testInvalidGetPartitionsByNames()
    {
        Map<String, Optional<Partition>> partitionsByNames = metastoreWithAllCachesEnabled.getPartitionsByNames(TEST_METASTORE_CONTEXT, BAD_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1));
        assertEquals(partitionsByNames.size(), 1);
        Optional<Partition> onlyElement = Iterables.getOnlyElement(partitionsByNames.values());
        assertFalse(onlyElement.isPresent());
    }

    @Test
    public void testNoCacheExceptions()
    {
        // Throw exceptions on usage
        mockClient.setThrowException(true);
        try {
            metastoreWithAllCachesEnabled.getAllDatabases(TEST_METASTORE_CONTEXT);
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockClient.getAccessCount(), 1);

        // Second try should hit the client again
        try {
            metastoreWithAllCachesEnabled.getAllDatabases(TEST_METASTORE_CONTEXT);
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test
    public void testTableConstraints()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        List<TableConstraint<String>> tableConstraints = metastoreWithAllCachesEnabled.getTableConstraints(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(tableConstraints.get(0), new PrimaryKeyConstraint<>(Optional.of("pk"), new LinkedHashSet<>(ImmutableList.of("c1")), true, true, false));
        assertEquals(tableConstraints.get(1), new UniqueConstraint<>(Optional.of("uk"), new LinkedHashSet<>(ImmutableList.of("c2")), true, true, false));
        assertEquals(tableConstraints.get(2), new NotNullConstraint<>("c3"));
        assertEquals(mockClient.getAccessCount(), 3);
        metastoreWithAllCachesEnabled.getTableConstraints(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(mockClient.getAccessCount(), 3);
        metastoreWithAllCachesEnabled.invalidateAll();
        metastoreWithAllCachesEnabled.getTableConstraints(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(mockClient.getAccessCount(), 6);

        // Test invalidate TEST_TABLE, which should not affect any entries linked to TEST_TABLE_WITH_CONSTRAINTS
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE);
        metastoreWithAllCachesEnabled.getTableConstraints(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(mockClient.getAccessCount(), 6);

        // Test invalidate TEST_TABLE_WITH_CONSTRAINTS
        metastoreWithAllCachesEnabled.invalidateCache(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        metastoreWithAllCachesEnabled.getTableConstraints(TEST_METASTORE_CONTEXT, TEST_DATABASE, TEST_TABLE_WITH_CONSTRAINTS);
        assertEquals(mockClient.getAccessCount(), 9);
    }

    public static class MockHiveCluster
            implements HiveCluster
    {
        private final MockHiveMetastoreClient client;

        private MockHiveCluster(MockHiveMetastoreClient client)
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
