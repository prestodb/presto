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
package com.facebook.presto.raptor.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.ShardManagerDaoUtils.createShardTablesWithRetry;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
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

        createShardTablesWithRetry(dao);
    }

    @AfterMethod
    public void teardown()
    {
        handle.close();
    }

    @Test
    public void testExternalBatches()
    {
        assertFalse(dao.externalBatchExists("foo"));
        assertFalse(dao.externalBatchExists("bar"));

        dao.insertExternalBatch("foo");

        assertTrue(dao.externalBatchExists("foo"));
        assertFalse(dao.externalBatchExists("bar"));

        try {
            dao.insertExternalBatch("foo");
            fail("expected exception");
        }
        catch (UnableToExecuteStatementException e) {
            assertInstanceOf(e.getCause(), SQLException.class);
            assertTrue(((SQLException) e.getCause()).getSQLState().startsWith("23"));
        }
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());
    }

    @Test
    public void testNodeInsert()
            throws Exception
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());

        String nodeName = UUID.randomUUID().toString();
        dao.insertNode(nodeName);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName));
    }

    @Test
    public void testInsertShard()
    {
        long shardId = dao.insertShard(UUID.randomUUID(), 5, 13, 42, 84);

        List<Map<String, Object>> shards = handle.select(
                "SELECT table_id , row_count, compressed_size, uncompressed_size FROM shards WHERE shard_id = ?",
                shardId);

        assertEquals(shards.size(), 1);
        Map<String, Object> shard = shards.get(0);

        assertEquals(shard.get("table_id"), 5L);
        assertEquals(shard.get("row_count"), 13L);
        assertEquals(shard.get("compressed_size"), 42L);
        assertEquals(shard.get("uncompressed_size"), 84L);
    }

    @Test
    public void testInsertShardNodeUsingShardUuid()
            throws Exception
    {
        dao.insertNode("node");
        int nodeId = dao.getNodeId("node");

        long tableId = 1;
        UUID shard = UUID.randomUUID();
        dao.insertShard(shard, tableId, 0, 0, 0);

        dao.insertShardNode(shard, nodeId);

        assertEquals(dao.getShardNodes(tableId), ImmutableList.of(new ShardNode(shard, "node")));
    }

    @Test
    public void testNodeShards()
            throws Exception
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());

        String nodeName1 = UUID.randomUUID().toString();
        dao.insertNode(nodeName1);
        Integer nodeId1 = dao.getNodeId(nodeName1);
        assertNotNull(nodeId1);

        String nodeName2 = UUID.randomUUID().toString();
        dao.insertNode(nodeName2);
        Integer nodeId2 = dao.getNodeId(nodeName2);
        assertNotNull(nodeId2);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName1, nodeName2));

        UUID shardUuid1 = UUID.randomUUID();
        UUID shardUuid2 = UUID.randomUUID();
        UUID shardUuid3 = UUID.randomUUID();
        UUID shardUuid4 = UUID.randomUUID();

        long tableId = 1;

        long shardId1 = dao.insertShard(shardUuid1, tableId, 0, 0, 0);
        long shardId2 = dao.insertShard(shardUuid2, tableId, 0, 0, 0);
        long shardId3 = dao.insertShard(shardUuid3, tableId, 0, 0, 0);
        long shardId4 = dao.insertShard(shardUuid4, tableId, 0, 0, 0);

        assertEquals(dao.getShards(tableId), ImmutableList.of(shardUuid1, shardUuid2, shardUuid3, shardUuid4));

        assertEquals(dao.getNodeShards(nodeName1).size(), 0);
        assertEquals(dao.getNodeShards(nodeName2).size(), 0);

        dao.insertShardNode(shardId1, nodeId1);
        dao.insertShardNode(shardId2, nodeId1);
        dao.insertShardNode(shardId3, nodeId1);
        dao.insertShardNode(shardId4, nodeId1);
        dao.insertShardNode(shardId1, nodeId2);
        dao.insertShardNode(shardId4, nodeId2);

        assertEquals(dao.getNodeShards(nodeName1), ImmutableList.of(shardUuid1, shardUuid2, shardUuid3, shardUuid4));
        assertEquals(dao.getNodeShards(nodeName2), ImmutableList.of(shardUuid1, shardUuid4));

        dao.dropShardNodes(tableId);

        assertEquals(dao.getShardNodes(tableId), ImmutableList.of());

        dao.dropShards(tableId);

        assertEquals(dao.getShards(tableId), ImmutableList.of());
    }

    @Test
    public void testShardSelection()
            throws Exception
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());

        String nodeName1 = UUID.randomUUID().toString();
        dao.insertNode(nodeName1);
        Integer nodeId1 = dao.getNodeId(nodeName1);
        assertNotNull(nodeId1);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName1));

        String nodeName2 = UUID.randomUUID().toString();
        dao.insertNode(nodeName2);
        Integer nodeId2 = dao.getNodeId(nodeName2);
        assertNotNull(nodeId2);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName1, nodeName2));

        long tableId = 1;

        UUID shardUuid1 = UUID.randomUUID();
        UUID shardUuid2 = UUID.randomUUID();
        UUID shardUuid3 = UUID.randomUUID();
        UUID shardUuid4 = UUID.randomUUID();

        long shardId1 = dao.insertShard(shardUuid1, tableId, 0, 0, 0);
        long shardId2 = dao.insertShard(shardUuid2, tableId, 0, 0, 0);
        long shardId3 = dao.insertShard(shardUuid3, tableId, 0, 0, 0);
        long shardId4 = dao.insertShard(shardUuid4, tableId, 0, 0, 0);

        List<UUID> shards = dao.getShards(tableId);
        assertEquals(shards.size(), 4);

        assertTrue(shards.contains(shardUuid1));
        assertTrue(shards.contains(shardUuid2));
        assertTrue(shards.contains(shardUuid3));
        assertTrue(shards.contains(shardUuid4));

        assertEquals(dao.getShardNodes(tableId).size(), 0);

        dao.insertShardNode(shardId1, nodeId1);
        dao.insertShardNode(shardId1, nodeId2);
        dao.insertShardNode(shardId2, nodeId1);
        dao.insertShardNode(shardId3, nodeId1);
        dao.insertShardNode(shardId4, nodeId1);
        dao.insertShardNode(shardId4, nodeId2);

        assertEquals(dao.getShards(tableId), shards);

        List<ShardNode> shardNodes = dao.getShardNodes(tableId);
        assertEquals(shardNodes.size(), 6);

        assertContainsShardNode(shardNodes, nodeName1, shardUuid1);
        assertContainsShardNode(shardNodes, nodeName2, shardUuid1);
        assertContainsShardNode(shardNodes, nodeName1, shardUuid2);
        assertContainsShardNode(shardNodes, nodeName1, shardUuid3);
        assertContainsShardNode(shardNodes, nodeName1, shardUuid4);
        assertContainsShardNode(shardNodes, nodeName2, shardUuid4);
    }

    private static void assertContainsShardNode(List<ShardNode> nodes, String nodeName, UUID shardUuid)
    {
        assertTrue(nodes.contains(new ShardNode(shardUuid, nodeName)));
    }
}
