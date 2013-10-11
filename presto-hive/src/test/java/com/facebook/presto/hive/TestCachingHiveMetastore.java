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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.MockHiveMetastoreClient.BAD_DATABASE;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_DATABASE;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_PARTITION1;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_PARTITION2;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_TABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestCachingHiveMetastore
{
    private MockHiveMetastoreClient mockClient;
    private CachingHiveMetastore metastore;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        mockClient = new MockHiveMetastoreClient();
        MockHiveCluster mockHiveCluster = new MockHiveCluster(mockClient);
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build()));
        metastore = new CachingHiveMetastore(mockHiveCluster, executor, new Duration(5, TimeUnit.MINUTES), new Duration(1, TimeUnit.MINUTES));
    }

    @Test
    public void testGetAllDatabases()
            throws Exception
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
            throws Exception
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test(expectedExceptions = NoSuchObjectException.class)
    public void testInvalidDbGetAllTAbles()
            throws Exception
    {
        metastore.getAllTables(BAD_DATABASE);
    }

    @Test
    public void testGetTable()
            throws Exception
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

    @Test(expectedExceptions = NoSuchObjectException.class)
    public void testInvalidDbGetTable()
            throws Exception
    {
        metastore.getTable(BAD_DATABASE, TEST_TABLE);
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test
    public void testInvalidGetPartitionNames()
            throws Exception
    {
        assertEquals(metastore.getPartitionNames(BAD_DATABASE, TEST_TABLE), ImmutableList.of());
    }

    @Test
    public void testGetPartitionNamesByParts()
            throws Exception
    {
        ImmutableList<String> parts = ImmutableList.of();
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getPartitionNamesByParts(TEST_DATABASE, TEST_TABLE, parts), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionNamesByParts(TEST_DATABASE, TEST_TABLE, parts), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getPartitionNamesByParts(TEST_DATABASE, TEST_TABLE, parts), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test(expectedExceptions = NoSuchObjectException.class)
    public void testInvalidGetPartitionNamesByParts()
            throws Exception
    {
        ImmutableList<String> parts = ImmutableList.of();
        metastore.getPartitionNamesByParts(BAD_DATABASE, TEST_TABLE, parts);
    }

    @Test
    public void testGetPartitionsByNames()
            throws Exception
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

    @Test(expectedExceptions = NoSuchObjectException.class)
    public void testInvalidGetPartitionsByNames()
            throws Exception
    {
        metastore.getPartitionsByNames(BAD_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1));
    }

    @Test
    public void testNoCacheExceptions()
            throws Exception
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

    private static class MockHiveCluster
            implements HiveCluster
    {
        private final HiveMetastoreClient client;

        private MockHiveCluster(HiveMetastoreClient client)
        {
            this.client = client;
        }

        @Override
        public HiveMetastoreClient createMetastoreClient()
        {
            return client;
        }
    }
}
