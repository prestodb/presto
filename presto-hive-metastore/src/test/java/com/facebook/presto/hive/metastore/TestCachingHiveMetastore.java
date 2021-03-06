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

import com.facebook.presto.hive.MockHiveMetastore;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.metastore.Partition.Builder;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.BAD_DATABASE;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.PARTITION_VERSION;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_DATABASE;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION1;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_PARTITION2;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_ROLES;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_TABLE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.function.UnaryOperator.identity;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCachingHiveMetastore
{
    private static final ImmutableList<String> EXPECTED_PARTITIONS = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);

    private MockHiveMetastoreClient mockClient;
    private CachingHiveMetastore metastore;
    private ThriftHiveMetastoreStats stats;

    @BeforeMethod
    public void setUp()
    {
        mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        ThriftHiveMetastore thriftHiveMetastore = new ThriftHiveMetastore(mockHiveCluster);
        PartitionMutator hivePartitionMutator = new HivePartitionMutator();
        metastore = new CachingHiveMetastore(
                new BridgingHiveMetastore(thriftHiveMetastore, hivePartitionMutator),
                executor,
                new Duration(5, TimeUnit.MINUTES),
                new Duration(1, TimeUnit.MINUTES),
                1000,
                false,
                MetastoreCacheScope.ALL);
        stats = thriftHiveMetastore.getStats();
    }

    @Test
    public void testGetAllDatabases()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test
    public void testGetAllTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllTables(TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllTables(TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getAllTables(TEST_DATABASE).get(), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
    }

    public void testInvalidDbGetAllTAbles()
    {
        assertFalse(metastore.getAllTables(BAD_DATABASE).isPresent());
    }

    @Test
    public void testGetTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
    }

    public void testInvalidDbGetTable()
    {
        assertFalse(metastore.getTable(BAD_DATABASE, TEST_TABLE).isPresent());

        assertEquals(stats.getGetTable().getThriftExceptions().getTotalCount(), 0);
        assertEquals(stats.getGetTable().getTotalFailures().getTotalCount(), 0);
    }

    @Test
    public void testGetPartitionNames()
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE).get(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test
    public void testInvalidGetPartitionNames()
    {
        assertEquals(metastore.getPartitionNames(BAD_DATABASE, TEST_TABLE).get(), ImmutableList.of());
    }

    @Test
    public void testGetPartitionNamesByParts()
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
    }

    private static class MockPartitionMutator
            implements PartitionMutator
    {
        private final Function<Integer, Integer> versionUpdater;
        private int version = PARTITION_VERSION;

        public MockPartitionMutator(Function<Integer, Integer> versionUpdater)
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
        CachingHiveMetastore partitionCachingEnabledmetastore = new CachingHiveMetastore(
                new BridgingHiveMetastore(mockHiveMetastore, mockPartitionMutator),
                executor,
                new Duration(5, TimeUnit.MINUTES),
                new Duration(1, TimeUnit.MINUTES),
                1000,
                true,
                MetastoreCacheScope.PARTITION);

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
        // Assert that we did not hit cache
        assertEquals(mockClient.getAccessCount(), 2);

        // Select all of the available partitions and load them into the cache
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        // Now if we fetch any or both of them, they should hit the cache
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION2)).size(), 1);
        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        // This call should NOT invalidate the partition cache because partition version is same as before
        assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
        assertEquals(mockClient.getAccessCount(), 4);

        assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        // Assert that its a cache hit
        assertEquals(mockClient.getAccessCount(), 4);

        assertInvalidateCache(new MockPartitionMutator(version -> version + 1));
        assertInvalidateCache(new MockPartitionMutator(version -> version - 1));
    }

    private void assertInvalidateCache(MockPartitionMutator partitionMutator)
    {
        MockHiveMetastoreClient mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("partition-versioning-test-%s")));
        MockHiveMetastore mockHiveMetastore = new MockHiveMetastore(mockHiveCluster);
        CachingHiveMetastore partitionCachingEnabledmetastore = new CachingHiveMetastore(
                new BridgingHiveMetastore(mockHiveMetastore, partitionMutator),
                executor,
                new Duration(5, TimeUnit.MINUTES),
                new Duration(1, TimeUnit.MINUTES),
                1000,
                true,
                MetastoreCacheScope.PARTITION);

        int clientAccessCount = 0;
        for (int i = 0; i < 100; i++) {
            assertEquals(partitionCachingEnabledmetastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, ImmutableMap.of()), EXPECTED_PARTITIONS);
            assertEquals(mockClient.getAccessCount(), ++clientAccessCount);
            assertEquals(partitionCachingEnabledmetastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
            // Assert that we did not hit cache
            assertEquals(mockClient.getAccessCount(), ++clientAccessCount);
        }
    }

    public void testInvalidGetPartitionNamesByParts()
    {
        assertTrue(metastore.getPartitionNamesByFilter(BAD_DATABASE, TEST_TABLE, ImmutableMap.of()).isEmpty());
    }

    @Test
    public void testGetPartitionsByNames()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        metastore.getTable(TEST_DATABASE, TEST_TABLE);
        assertEquals(mockClient.getAccessCount(), 1);

        // Select half of the available partitions and load them into the cache
        assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        assertEquals(mockClient.getAccessCount(), 2);

        // Now select all of the partitions
        assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        // There should be one more access to fetch the remaining partition
        assertEquals(mockClient.getAccessCount(), 3);

        // Now if we fetch any or both of them, they should not hit the client
        assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION2)).size(), 1);
        assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        metastore.flushCache();

        // Fetching both should only result in one batched access
        assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 4);
    }

    @Test
    public void testListRoles()
            throws Exception
    {
        assertEquals(mockClient.getAccessCount(), 0);

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 2);

        metastore.createRole("role", "grantor");

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 3);

        metastore.dropRole("testrole");

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 4);
    }

    public void testInvalidGetPartitionsByNames()
    {
        Map<String, Optional<Partition>> partitionsByNames = metastore.getPartitionsByNames(BAD_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1));
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
            metastore.getAllDatabases();
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockClient.getAccessCount(), 1);

        // Second try should hit the client again
        try {
            metastore.getAllDatabases();
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockClient.getAccessCount(), 2);
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
        public HiveMetastoreClient createMetastoreClient()
        {
            return client;
        }

        public MockHiveMetastoreClient createPartitionVersionSupportedMetastoreClient()
        {
            return client;
        }
    }
}
