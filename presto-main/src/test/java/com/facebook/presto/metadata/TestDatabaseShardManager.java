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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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
        UUID shardId1 = UUID.randomUUID();
        UUID shardId2 = UUID.randomUUID();

        assertNotEquals(shardId2, shardId1);

        Set<String> nodes = shardManager.getTableNodes(tableHandle);
        assertTrue(nodes.isEmpty());

        shardManager.commitPartition(tableHandle, "some-partition", ImmutableList.<PartitionKey>of(), ImmutableMap.of(shardId1, "some-node"));
        shardManager.commitPartition(tableHandle, "some-other-partition", ImmutableList.<PartitionKey>of(), ImmutableMap.of(shardId2, "some-node"));

        nodes = shardManager.getTableNodes(tableHandle);
        assertEquals(nodes, ImmutableSet.of("some-node"));

        Set<TablePartition> partitions = shardManager.getPartitions(tableHandle);
        assertEquals(partitions.size(), 2);

        long partitionId = partitions.iterator().next().getPartitionId();

        Multimap<Long, Entry<UUID, String>> allShardNodes = shardManager.getShardNodesByPartition(tableHandle);
        assertNotNull(allShardNodes);
        assertEquals(allShardNodes.size(), 2);
        Collection<Entry<UUID, String>> partitionShards = allShardNodes.get(partitionId);
        assertEquals(partitionShards.size(), 1);
    }
}
