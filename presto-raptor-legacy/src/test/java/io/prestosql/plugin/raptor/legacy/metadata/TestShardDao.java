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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestShardDao
{
    private TestingShardDao dao;
    private IDBI dbi;
    private Handle dummyHandle;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dao = dbi.onDemand(TestingShardDao.class);
        createTablesWithRetry(dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        dummyHandle.close();
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
    public void testInsertCreatedShard()
    {
        long transactionId = dao.insertTransaction();
        dao.insertCreatedShard(UUID.randomUUID(), transactionId);
        dao.deleteCreatedShards(transactionId);
    }

    @Test
    public void testInsertDeletedShards()
    {
        dao.insertDeletedShards(ImmutableList.of(UUID.randomUUID(), UUID.randomUUID()));
        dao.insertDeletedShards(0);
    }

    @Test
    public void testNodeInsert()
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());

        String nodeName = UUID.randomUUID().toString();
        int nodeId = dao.insertNode(nodeName);
        assertEquals(dao.getNodeId(nodeName), (Integer) nodeId);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName));
    }

    @Test
    public void testInsertShard()
    {
        long tableId = createTable("test");
        UUID shardUuid = UUID.randomUUID();
        long shardId = dao.insertShard(shardUuid, tableId, null, 13, 42, 84, 1234);

        ShardMetadata shard = dao.getShard(shardUuid);
        assertNotNull(shard);
        assertEquals(shard.getTableId(), tableId);
        assertEquals(shard.getShardId(), shardId);
        assertEquals(shard.getShardUuid(), shardUuid);
        assertEquals(shard.getRowCount(), 13);
        assertEquals(shard.getCompressedSize(), 42);
        assertEquals(shard.getUncompressedSize(), 84);
        assertEquals(shard.getXxhash64(), OptionalLong.of(1234));
    }

    @Test
    public void testInsertShardNodeUsingShardUuid()
    {
        int nodeId = dao.insertNode("node");

        long tableId = createTable("test");
        UUID shard = UUID.randomUUID();
        dao.insertShard(shard, tableId, null, 0, 0, 0, 0);

        dao.insertShardNode(shard, nodeId);

        assertEquals(dao.getShardNodes(tableId), ImmutableList.of(new ShardNode(shard, "node")));
    }

    @Test
    public void testNodeShards()
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());

        String nodeName1 = UUID.randomUUID().toString();
        int nodeId1 = dao.insertNode(nodeName1);

        String nodeName2 = UUID.randomUUID().toString();
        int nodeId2 = dao.insertNode(nodeName2);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName1, nodeName2));

        UUID shardUuid1 = UUID.randomUUID();
        UUID shardUuid2 = UUID.randomUUID();
        UUID shardUuid3 = UUID.randomUUID();
        UUID shardUuid4 = UUID.randomUUID();
        UUID shardUuid5 = UUID.randomUUID();

        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        int bucketCount = 20;
        long distributionId = metadataDao.insertDistribution("test", "bigint", bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            Integer nodeId = ((i % 2) == 0) ? nodeId1 : nodeId2;
            dao.insertBuckets(distributionId, ImmutableList.of(i), ImmutableList.of(nodeId));
        }

        long plainTableId = metadataDao.insertTable("test", "plain", false, false, null, 0);
        long bucketedTableId = metadataDao.insertTable("test", "bucketed", false, false, distributionId, 0);

        long shardId1 = dao.insertShard(shardUuid1, plainTableId, null, 1, 11, 111, 888_111);
        long shardId2 = dao.insertShard(shardUuid2, plainTableId, null, 2, 22, 222, 888_222);
        long shardId3 = dao.insertShard(shardUuid3, bucketedTableId, 8, 3, 33, 333, 888_333);
        long shardId4 = dao.insertShard(shardUuid4, bucketedTableId, 9, 4, 44, 444, 888_444);
        long shardId5 = dao.insertShard(shardUuid5, bucketedTableId, 7, 5, 55, 555, 888_555);

        OptionalInt noBucket = OptionalInt.empty();
        OptionalLong noRange = OptionalLong.empty();
        ShardMetadata shard1 = new ShardMetadata(plainTableId, shardId1, shardUuid1, noBucket, 1, 11, 111, OptionalLong.of(888_111), noRange, noRange);
        ShardMetadata shard2 = new ShardMetadata(plainTableId, shardId2, shardUuid2, noBucket, 2, 22, 222, OptionalLong.of(888_222), noRange, noRange);
        ShardMetadata shard3 = new ShardMetadata(bucketedTableId, shardId3, shardUuid3, OptionalInt.of(8), 3, 33, 333, OptionalLong.of(888_333), noRange, noRange);
        ShardMetadata shard4 = new ShardMetadata(bucketedTableId, shardId4, shardUuid4, OptionalInt.of(9), 4, 44, 444, OptionalLong.of(888_444), noRange, noRange);
        ShardMetadata shard5 = new ShardMetadata(bucketedTableId, shardId5, shardUuid5, OptionalInt.of(7), 5, 55, 555, OptionalLong.of(888_555), noRange, noRange);

        assertEquals(dao.getShards(plainTableId), ImmutableSet.of(shardUuid1, shardUuid2));
        assertEquals(dao.getShards(bucketedTableId), ImmutableSet.of(shardUuid3, shardUuid4, shardUuid5));

        assertEquals(dao.getNodeShards(nodeName1, null), ImmutableSet.of(shard3));
        assertEquals(dao.getNodeShards(nodeName2, null), ImmutableSet.of(shard4, shard5));
        assertEquals(dao.getNodeSizes(), ImmutableSet.of(
                new NodeSize(nodeName1, 33),
                new NodeSize(nodeName2, 44 + 55)));

        dao.insertShardNode(shardId1, nodeId1);
        dao.insertShardNode(shardId2, nodeId1);
        dao.insertShardNode(shardId1, nodeId2);

        assertEquals(dao.getNodeShards(nodeName1, null), ImmutableSet.of(shard1, shard2, shard3));
        assertEquals(dao.getNodeShards(nodeName2, null), ImmutableSet.of(shard1, shard4, shard5));
        assertEquals(dao.getNodeSizes(), ImmutableSet.of(
                new NodeSize(nodeName1, 11 + 22 + 33),
                new NodeSize(nodeName2, 11 + 44 + 55)));

        dao.dropShardNodes(plainTableId);

        assertEquals(dao.getNodeShards(nodeName1, null), ImmutableSet.of(shard3));
        assertEquals(dao.getNodeShards(nodeName2, null), ImmutableSet.of(shard4, shard5));
        assertEquals(dao.getNodeSizes(), ImmutableSet.of(
                new NodeSize(nodeName1, 33),
                new NodeSize(nodeName2, 44 + 55)));

        dao.dropShards(plainTableId);
        dao.dropShards(bucketedTableId);

        assertEquals(dao.getShards(plainTableId), ImmutableSet.of());
        assertEquals(dao.getShards(bucketedTableId), ImmutableSet.of());
        assertEquals(dao.getNodeSizes(), ImmutableSet.of());
    }

    @Test
    public void testShardSelection()
    {
        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of());

        String nodeName1 = UUID.randomUUID().toString();
        int nodeId1 = dao.insertNode(nodeName1);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName1));

        String nodeName2 = UUID.randomUUID().toString();
        int nodeId2 = dao.insertNode(nodeName2);

        assertEquals(dao.getAllNodesInUse(), ImmutableSet.of(nodeName1, nodeName2));

        long tableId = createTable("test");

        UUID shardUuid1 = UUID.randomUUID();
        UUID shardUuid2 = UUID.randomUUID();
        UUID shardUuid3 = UUID.randomUUID();
        UUID shardUuid4 = UUID.randomUUID();

        long shardId1 = dao.insertShard(shardUuid1, tableId, null, 0, 0, 0, 0);
        long shardId2 = dao.insertShard(shardUuid2, tableId, null, 0, 0, 0, 0);
        long shardId3 = dao.insertShard(shardUuid3, tableId, null, 0, 0, 0, 0);
        long shardId4 = dao.insertShard(shardUuid4, tableId, null, 0, 0, 0, 0);

        Set<UUID> shards = dao.getShards(tableId);
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

        Set<ShardNode> shardNodes = dao.getShardNodes(tableId);
        assertEquals(shardNodes.size(), 6);

        assertContainsShardNode(shardNodes, nodeName1, shardUuid1);
        assertContainsShardNode(shardNodes, nodeName2, shardUuid1);
        assertContainsShardNode(shardNodes, nodeName1, shardUuid2);
        assertContainsShardNode(shardNodes, nodeName1, shardUuid3);
        assertContainsShardNode(shardNodes, nodeName1, shardUuid4);
        assertContainsShardNode(shardNodes, nodeName2, shardUuid4);
    }

    private long createTable(String name)
    {
        return dbi.onDemand(MetadataDao.class).insertTable("test", name, false, false, null, 0);
    }

    private static void assertContainsShardNode(Set<ShardNode> nodes, String nodeName, UUID shardUuid)
    {
        assertTrue(nodes.contains(new ShardNode(shardUuid, nodeName)));
    }
}
