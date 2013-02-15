package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.MockHiveMetastoreClient.BAD_DATABASE;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_DATABASE;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_PARTITION1;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_PARTITION2;
import static com.facebook.presto.hive.MockHiveMetastoreClient.TEST_TABLE;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestCachingHiveMetastore
{
    private MockHiveMetastoreClient mockClient;
    private MockHiveCluster mockHiveCluster;
    private CachingHiveMetastore metastore;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        mockClient = new MockHiveMetastoreClient();
        mockHiveCluster = new MockHiveCluster(mockClient);
        metastore = new CachingHiveMetastore(mockHiveCluster, new Duration(5, TimeUnit.MINUTES));
    }

    @Test
    public void testGetAllDatabases()
            throws Exception
    {
        Assert.assertEquals(mockClient.getAccessCount(), 0);
        Assert.assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        Assert.assertEquals(mockClient.getAccessCount(), 1);
        Assert.assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        Assert.assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        Assert.assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test
    public void testGetAllTable()
            throws Exception
    {
        Assert.assertEquals(mockClient.getAccessCount(), 0);
        Assert.assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        Assert.assertEquals(mockClient.getAccessCount(), 1);
        Assert.assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        Assert.assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        Assert.assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test(expectedExceptions = NoSuchObjectException.class)
    public void testInvalidDbGetAllTAbles()
            throws Exception
    {
        metastore.getAllTables(MockHiveMetastoreClient.BAD_DATABASE);
    }

    @Test
    public void testGetTable()
            throws Exception
    {
        Assert.assertEquals(mockClient.getAccessCount(), 0);
        Assert.assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        Assert.assertEquals(mockClient.getAccessCount(), 1);
        Assert.assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        Assert.assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        Assert.assertEquals(mockClient.getAccessCount(), 2);
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
        Assert.assertEquals(mockClient.getAccessCount(), 0);
        Assert.assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE), expectedPartitions);
        Assert.assertEquals(mockClient.getAccessCount(), 1);
        Assert.assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE), expectedPartitions);
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        Assert.assertEquals(metastore.getPartitionNames(TEST_DATABASE, TEST_TABLE), expectedPartitions);
        Assert.assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test(expectedExceptions = NoSuchObjectException.class)
    public void testInvalidGetPartitionNames()
            throws Exception
    {
        metastore.getPartitionNames(BAD_DATABASE, TEST_TABLE);
    }

    @Test
    public void testGetPartitionNamesByParts()
            throws Exception
    {
        ImmutableList<String> parts = ImmutableList.of();
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);

        Assert.assertEquals(mockClient.getAccessCount(), 0);
        Assert.assertEquals(metastore.getPartitionNamesByParts(TEST_DATABASE, TEST_TABLE, parts), expectedPartitions);
        Assert.assertEquals(mockClient.getAccessCount(), 1);
        Assert.assertEquals(metastore.getPartitionNamesByParts(TEST_DATABASE, TEST_TABLE, parts), expectedPartitions);
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        Assert.assertEquals(metastore.getPartitionNamesByParts(TEST_DATABASE, TEST_TABLE, parts), expectedPartitions);
        Assert.assertEquals(mockClient.getAccessCount(), 2);
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
        Assert.assertEquals(mockClient.getAccessCount(), 0);

        // Select half of the available partitions and load them into the cache
        Assert.assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        // Now select all of the partitions
        Assert.assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        // There should be one more access to fetch the remaining partition
        Assert.assertEquals(mockClient.getAccessCount(), 2);

        // Now if we fetch any or both of them, they should not hit the client
        Assert.assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        Assert.assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION2)).size(), 1);
        Assert.assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        Assert.assertEquals(mockClient.getAccessCount(), 2);

        metastore.flushCache();

        // Fetching both should only result in one batched access
        Assert.assertEquals(metastore.getPartitionsByNames(TEST_DATABASE, TEST_TABLE, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        Assert.assertEquals(mockClient.getAccessCount(), 3);
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
        catch (Exception e) {
            assertInstanceOf(e, RuntimeException.class);
        }
        Assert.assertEquals(mockClient.getAccessCount(), 1);

        // Second try should hit the client again
        try {
            metastore.getAllDatabases();
        }
        catch (Exception e) {
            assertInstanceOf(e, RuntimeException.class);
        }
        Assert.assertEquals(mockClient.getAccessCount(), 2);
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
