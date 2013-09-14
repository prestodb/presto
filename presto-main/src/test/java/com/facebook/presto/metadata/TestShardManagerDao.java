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

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.split.NativePartitionKey;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class TestShardManagerDao
{
    private ShardManagerDao dao;
    private Handle handle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        H2EmbeddedDataSourceConfig dataSourceConfig = new H2EmbeddedDataSourceConfig().setFilename("mem:");
        DataSource dataSource = new H2EmbeddedDataSource(dataSourceConfig);
        DBI h2Dbi = new DBI(dataSource);
        handle = h2Dbi.open();
        dao = handle.attach(ShardManagerDao.class);

        ShardManagerDao.Utils.createShardTablesWithRetry(dao);
    }

    @AfterMethod
    public void teardown()
    {
        handle.close();
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        List<String> nodes = dao.getAllNodesInUse();
        assertNotNull(nodes);
        assertEquals(nodes.size(), 0);
    }

    @Test
    public void testNodeInsert()
            throws Exception
    {
        List<String> nodes = dao.getAllNodesInUse();
        assertNotNull(nodes);
        assertEquals(nodes.size(), 0);

        String nodeName = UUID.randomUUID().toString();
        dao.insertNode(nodeName);

        nodes = dao.getAllNodesInUse();
        assertNotNull(nodes);
        assertEquals(nodes.size(), 1);
        assertEquals(nodes.get(0), nodeName);
    }

    @Test
    public void testShardSelection()
            throws Exception
    {
        String nodeName = UUID.randomUUID().toString();
        dao.insertNode(nodeName);
        Long nodeId = dao.getNodeId(nodeName);
        assertNotNull(nodeId);

        long tableId = 1;

        long partitionId0 = dao.insertPartition(tableId, "part_0");
        long partitionId1 = dao.insertPartition(tableId, "part_1");
        long partitionId2 = dao.insertPartition(tableId, "part_2");

        long shardId0 = dao.insertShard(tableId, true);
        long shardId1 = dao.insertShard(tableId, true);
        long shardId2 = dao.insertShard(tableId, true);

        dao.insertShardNode(shardId0, nodeId);
        dao.insertShardNode(shardId1, nodeId);
        dao.insertShardNode(shardId2, nodeId);

        dao.insertPartitionShard(shardId0, tableId, partitionId0);
        dao.insertPartitionShard(shardId1, tableId, partitionId1);
        dao.insertPartitionShard(shardId2, tableId, partitionId2);

        Set<TablePartition> partitions = dao.getPartitions(tableId);
        assertEquals(partitions.size(), 3);

        List<ShardNode> partitionNodes = dao.getCommittedShardNodesByPartitionId(partitionId1);
        assertEquals(partitionNodes.size(), 1);

        List<ShardNode> tableNodes = dao.getCommittedShardNodesByTableId(tableId);
        assertEquals(tableNodes.size(), 3);
    }

    @Test
    public void testPartitionKey()
    {
        long tableId0 = 1;
        long tableId1 = 2;

        String partitionName0 = "ds=2013-06-01";
        String partitionName1 = "foo=bar";

        dao.insertPartition(tableId0, partitionName0);
        dao.insertPartition(tableId1, partitionName1);

        PartitionKey testKey0 = new NativePartitionKey(partitionName0, "ds", ColumnType.STRING, "2013-06-01");
        PartitionKey testKey1 = new NativePartitionKey(partitionName1, "foo", ColumnType.STRING, "bar");

        long keyId0 = dao.insertPartitionKey(tableId0, partitionName0, testKey0.getName(), testKey0.getType().toString(), testKey0.getValue());

        long keyId1 = dao.insertPartitionKey(tableId0, partitionName0, testKey1.getName(), testKey1.getType().toString(), testKey1.getValue());
        assertNotEquals(keyId1, keyId0);

        long keyId2 = dao.insertPartitionKey(tableId1, partitionName1, testKey1.getName(), testKey1.getType().toString(), testKey1.getValue());
        assertNotEquals(keyId2, keyId0);

        Set<NativePartitionKey> filteredKeys = ImmutableSet.copyOf(Collections2.filter(dao.getPartitionKeys(tableId1), NativePartitionKey.partitionNamePredicate(partitionName1)));
        assertEquals(filteredKeys.size(), 1);

        Set<NativePartitionKey> retrievedKeys = dao.getPartitionKeys(tableId1);
        assertNotNull(retrievedKeys);
        assertEquals(retrievedKeys.size(), 1);
        assertEquals(Iterables.getOnlyElement(retrievedKeys), testKey1);
    }
}
