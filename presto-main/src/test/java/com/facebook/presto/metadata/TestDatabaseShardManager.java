package com.facebook.presto.metadata;

import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class TestDatabaseShardManager
{
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        shardManager = new DatabaseShardManager(dbi);
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testPartitionShardCommit()
            throws Exception
    {
        long tableId = 1;
        TableHandle tableHandle = new NativeTableHandle("demo", "test", tableId);
        long shardId1 = shardManager.allocateShard(tableHandle);
        long shardId2 = shardManager.allocateShard(tableHandle);

        assertNotEquals(shardId2, shardId1);

        Multimap<Long, String> shardNodes = shardManager.getCommittedShardNodesByTableId(tableHandle);
        assertNotNull(shardNodes);
        assertEquals(shardNodes.size(), 0);

        shardManager.commitPartition(tableHandle, "some-partition", ImmutableList.<PartitionKey>of(), ImmutableMap.of(shardId1, "some-node"));
        shardManager.commitPartition(tableHandle, "some-other-partition", ImmutableList.<PartitionKey>of(), ImmutableMap.of(shardId2, "some-node"));

        shardNodes = shardManager.getCommittedShardNodesByTableId(tableHandle);
        assertNotNull(shardNodes);
        assertEquals(shardNodes.size(), 2);
        assertNotNull(shardNodes.get(shardId1));
        assertNotNull(shardNodes.get(shardId2));

        Set<TablePartition> partitions = shardManager.getPartitions(tableHandle);
        assertNotNull(partitions);
        assertEquals(partitions.size(), 2);

        long partitionId = partitions.iterator().next().getPartitionId();

        Multimap<Long, Entry<Long, String>> allShardNodes = shardManager.getCommittedPartitionShardNodes(tableHandle);
        assertNotNull(allShardNodes);
        assertEquals(allShardNodes.size(), 2);
        Collection<Entry<Long, String>> partitionShards = allShardNodes.get(partitionId);
        assertEquals(partitionShards.size(), 1);
    }
}
