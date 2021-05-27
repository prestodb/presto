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

import com.facebook.airlift.testing.TestingTicker;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.ResultIterator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.common.predicate.Range.lessThan;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestDatabaseShardManager
{
    private IDBI dbi;
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        dataDir = Files.createTempDir();
        shardManager = createShardManager(dbi);
    }

    @AfterMethod
    public void teardown()
            throws IOException
    {
        dummyHandle.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateTable()
            throws SQLException
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        Statement statement = dummyHandle.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("select * from " + shardIndexTable(tableId));
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(metaData.getColumnLabel(1), "SHARD_ID");
        assertEquals(metaData.getColumnLabel(2), "SHARD_UUID");
        assertNotEquals(metaData.getColumnLabel(3), "DELTA_SHARD_UUID");
        resultSet.close();
        statement.close();
    }

    @Test
    public void testCreateTableWithDeltaDelete()
            throws SQLException
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), true);

        Statement statement = dummyHandle.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("select * from " + shardIndexTable(tableId));
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(metaData.getColumnLabel(1), "SHARD_ID");
        assertEquals(metaData.getColumnLabel(2), "SHARD_UUID");
        assertEquals(metaData.getColumnLabel(3), "DELTA_SHARD_UUID");
        resultSet.close();
        statement.close();
    }

    @Test
    public void testCommit()
    {
        long tableId = createTable("test");

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node2"))
                .build();

        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        Set<ShardNodes> actual = getShardNodes(tableId, TupleDomain.all());
        assertEquals(actual, toShardNodes(shards));
    }

    @Test
    public void testRollback()
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        List<ShardInfo> shards = ImmutableList.of(shardInfo(UUID.randomUUID(), "node1"));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.rollbackTransaction(transactionId);

        try {
            shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
    }

    @Test
    public void testAssignShard()
    {
        long tableId = createTable("test");
        UUID shard = UUID.randomUUID();
        List<ShardInfo> shardNodes = ImmutableList.of(shardInfo(shard, "node1"));
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shardNodes, Optional.empty(), 0);

        ShardNodes actual = getOnlyElement(getShardNodes(tableId, TupleDomain.all()));
        assertEquals(actual, new ShardNodes(shard, Optional.empty(), ImmutableSet.of("node1")));

        try {
            shardManager.replaceShardAssignment(tableId, shard, Optional.empty(), "node2", true);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), SERVER_STARTING_UP.toErrorCode());
        }

        // replace shard assignment to another node
        shardManager.replaceShardAssignment(tableId, shard, Optional.empty(), "node2", false);

        actual = getOnlyElement(getShardNodes(tableId, TupleDomain.all()));
        assertEquals(actual, new ShardNodes(shard, Optional.empty(), ImmutableSet.of("node2")));

        // replacing shard assignment should be idempotent
        shardManager.replaceShardAssignment(tableId, shard, Optional.empty(), "node2", false);

        actual = getOnlyElement(getShardNodes(tableId, TupleDomain.all()));
        assertEquals(actual, new ShardNodes(shard, Optional.empty(), ImmutableSet.of("node2")));
    }

    @Test
    public void testGetNodeBytes()
    {
        long tableId = createTable("test");
        OptionalInt bucketNumber = OptionalInt.empty();

        UUID shard1 = UUID.randomUUID();
        UUID shard2 = UUID.randomUUID();
        List<ShardInfo> shardNodes = ImmutableList.of(
                new ShardInfo(shard1, bucketNumber, ImmutableSet.of("node1"), ImmutableList.of(), 3, 33, 333, 0),
                new ShardInfo(shard2, bucketNumber, ImmutableSet.of("node1"), ImmutableList.of(), 5, 55, 555, 0));
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shardNodes, Optional.empty(), 0);

        assertEquals(getShardNodes(tableId, TupleDomain.all()), ImmutableSet.of(
                new ShardNodes(shard1, Optional.empty(), ImmutableSet.of("node1")),
                new ShardNodes(shard2, Optional.empty(), ImmutableSet.of("node1"))));

        assertEquals(shardManager.getNodeBytes(), ImmutableMap.of("node1", 88L));

        shardManager.replaceShardAssignment(tableId, shard1, Optional.empty(), "node2", false);

        assertEquals(getShardNodes(tableId, TupleDomain.all()), ImmutableSet.of(
                new ShardNodes(shard1, Optional.empty(), ImmutableSet.of("node2")),
                new ShardNodes(shard2, Optional.empty(), ImmutableSet.of("node1"))));

        assertEquals(shardManager.getNodeBytes(), ImmutableMap.of("node1", 55L, "node2", 33L));
    }

    @Test
    public void testGetNodeTableShards()
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");

        ImmutableList.Builder<ShardInfo> inputShards = ImmutableList.builder();
        Multimap<String, UUID> nodeShardMap = HashMultimap.create();
        for (String node : nodes) {
            UUID uuid = UUID.randomUUID();
            nodeShardMap.put(node, uuid);
            inputShards.add(shardInfo(uuid, node));
        }

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, inputShards.build(), Optional.empty(), 0);

        for (String node : nodes) {
            Set<ShardMetadata> shardMetadata = shardManager.getNodeShardsAndDeltas(node);
            Set<UUID> expectedUuids = ImmutableSet.copyOf(nodeShardMap.get(node));
            Set<UUID> actualUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());
            assertEquals(actualUuids, expectedUuids);
        }
    }

    @Test
    public void testGetExistingShards()
    {
        long tableId = createTable("test");
        UUID shard1 = UUID.randomUUID();
        UUID shard2 = UUID.randomUUID();
        List<ShardInfo> shardNodes = ImmutableList.of(shardInfo(shard1, "node1"), shardInfo(shard2, "node1"));
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shardNodes, Optional.empty(), 0);
        Set<UUID> actual = shardManager.getExistingShardUuids(tableId, ImmutableSet.of(shard1, shard2, UUID.randomUUID()));
        Set<UUID> expected = ImmutableSet.of(shard1, shard2);
        assertEquals(actual, expected);
    }

    @Test
    public void testReplaceShardUuidsFunction()
            throws SQLException
    {
        // node1 shard1 shard4
        // node2 shard2
        // node3 shard3

        // goal: replace shard1 and shard4 with newUuid5

        // Initial data
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        UUID uuid4 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, OptionalInt.empty(), ImmutableSet.of("node1"), ImmutableList.of(), 1, 1, 1, 1);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, OptionalInt.empty(), ImmutableSet.of("node2"), ImmutableList.of(), 2, 2, 2, 2);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, OptionalInt.empty(), ImmutableSet.of("node3"), ImmutableList.of(), 3, 3, 3, 3);
        ShardInfo shardInfo4 = new ShardInfo(uuid4, OptionalInt.empty(), ImmutableSet.of("node1"), ImmutableList.of(), 4, 4, 4, 4);

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), true);
        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, ImmutableList.of(shardInfo1, shardInfo2, shardInfo3, shardInfo4), Optional.empty(), 0);

        // New data
        UUID newUuid5 = UUID.randomUUID();
        ShardInfo newShardInfo4 = new ShardInfo(newUuid5, OptionalInt.empty(), ImmutableSet.of("node1"), ImmutableList.of(), 5, 5, 5, 5);

        // toReplace
        Set<ShardMetadata> shardMetadata = shardManager.getNodeShardsAndDeltas("node1");
        Set<UUID> replacedUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());
        Map<UUID, Optional<UUID>> replaceUuidMap = replacedUuids.stream().collect(Collectors.toMap(uuid -> uuid, uuid -> Optional.empty()));

        transactionId = shardManager.beginTransaction();
        shardManager.replaceShardUuids(transactionId, tableId, columns, replaceUuidMap, ImmutableList.of(newShardInfo4), OptionalLong.of(0), true);

        // check shards on this node1 are correct
        shardMetadata = shardManager.getNodeShardsAndDeltas("node1");
        assertEquals(shardMetadata.size(), 1);
        for (ShardMetadata actual : shardMetadata) {
            assertEquals(actual.getShardUuid(), newUuid5);
            assertEquals(actual.getDeltaUuid(), Optional.empty());
            assertEquals(actual.getRowCount(), 5);
            assertEquals(actual.getCompressedSize(), 5);
            assertEquals(actual.getUncompressedSize(), 5);
        }

        // check that shards are replaced in index table as well
        Set<BucketShards> shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.all(), true));
        Set<UUID> actualAllUuids = shardNodes.stream()
                .map(BucketShards::getShards)
                .flatMap(Collection::stream)
                .map(ShardNodes::getShardUuid)
                .collect(toSet());
        Set<UUID> expectedAllUuids = ImmutableSet.of(uuid2, uuid3, newUuid5);
        assertEquals(actualAllUuids, expectedAllUuids);

        // Verify statistics
        Statement statement = dummyHandle.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM tables where table_id = " + tableId);
        resultSet.next();
        assertEquals(resultSet.getLong("shard_count"), 3);
        assertEquals(resultSet.getLong("delta_count"), 0);
        assertEquals(resultSet.getLong("row_count"), 10);
        assertEquals(resultSet.getLong("compressed_size"), 10);
        assertEquals(resultSet.getLong("uncompressed_size"), 10);
        resultSet.close();
        statement.close();
    }

    @Test
    public void testReplaceShardUuids()
    {
        // node1 shard1 / node2 shard2 / node3 shard3
        // replace shard1 with two new shard
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");
        List<UUID> originalUuids = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        List<ShardInfo> oldShards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(originalUuids.get(0), nodes.get(0)))
                .add(shardInfo(originalUuids.get(1), nodes.get(1)))
                .add(shardInfo(originalUuids.get(2), nodes.get(2)))
                .build();

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), true);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, oldShards, Optional.empty(), 0);

        // newShards
        List<UUID> expectedUuids = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID());
        List<ShardInfo> newShards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(expectedUuids.get(0), nodes.get(0)))
                .add(shardInfo(expectedUuids.get(1), nodes.get(0)))
                .build();

        // toReplace
        Set<ShardMetadata> shardMetadata = shardManager.getNodeShardsAndDeltas(nodes.get(0));
        Set<UUID> replacedUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());
        Map<UUID, Optional<UUID>> replaceUuidMap = replacedUuids.stream().collect(Collectors.toMap(uuid -> uuid, uuid -> Optional.empty()));

        transactionId = shardManager.beginTransaction();
        shardManager.replaceShardUuids(transactionId, tableId, columns, replaceUuidMap, newShards, OptionalLong.of(0), true);

        // check that shards are replaced in shards table for node1
        shardMetadata = shardManager.getNodeShardsAndDeltas(nodes.get(0));
        Set<UUID> actualUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());
        assertEquals(actualUuids, ImmutableSet.copyOf(expectedUuids));

        // Compute expected all uuids for this table
        Set<UUID> expectedAllUuids = new HashSet<>(originalUuids);
        expectedAllUuids.removeAll(replacedUuids);
        expectedAllUuids.addAll(expectedUuids);

        // check that shards are replaced in index table as well
        Set<BucketShards> shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.all(), true));
        Set<UUID> actualAllUuids = shardNodes.stream()
                .map(BucketShards::getShards)
                .flatMap(Collection::stream)
                .map(ShardNodes::getShardUuid)
                .collect(toSet());
        assertEquals(actualAllUuids, expectedAllUuids);

        // Verify conflict is handled
        // Try to replace shard1 with newShards again (shard1 already deleted, delete shards that's already deleted)
        try {
            newShards = ImmutableList.of(shardInfo(UUID.randomUUID(), nodes.get(0)));
            transactionId = shardManager.beginTransaction();
            shardManager.replaceShardUuids(transactionId, tableId, columns, replaceUuidMap, newShards, OptionalLong.of(0), true);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
        // Try to add new delta to shard1 (shard1 already deleted)
        try {
            transactionId = shardManager.beginTransaction();
            ShardInfo newDelta = shardInfo(UUID.randomUUID(), nodes.get(0));
            Map<UUID, DeltaInfoPair> shardMap = ImmutableMap.of(originalUuids.get(0), new DeltaInfoPair(Optional.empty(), Optional.of(newDelta)));
            shardManager.replaceDeltaUuids(transactionId, tableId, columns, shardMap, OptionalLong.of(0));
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
        // Try to delete shard1 (shard1 already deleted)
        try {
            transactionId = shardManager.beginTransaction();
            Map<UUID, DeltaInfoPair> shardMap = ImmutableMap.of(originalUuids.get(0), new DeltaInfoPair(Optional.empty(), Optional.empty()));
            shardManager.replaceDeltaUuids(transactionId, tableId, columns, shardMap, OptionalLong.of(0));
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
    }

    @Test
    public void testReplaceDeltaUuidsFunction()
            throws SQLException
    {
        // node1 shard1 shard4
        // node2 shard2
        // node3 shard3

        // goal: shard4 add delta1

        // Initial data
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        UUID uuid4 = UUID.randomUUID();
        ShardInfo shardInfo1 = new ShardInfo(uuid1, OptionalInt.empty(), ImmutableSet.of("node1"), ImmutableList.of(), 1, 1, 1, 1);
        ShardInfo shardInfo2 = new ShardInfo(uuid2, OptionalInt.empty(), ImmutableSet.of("node2"), ImmutableList.of(), 2, 2, 2, 2);
        ShardInfo shardInfo3 = new ShardInfo(uuid3, OptionalInt.empty(), ImmutableSet.of("node3"), ImmutableList.of(), 3, 3, 3, 3);
        ShardInfo shardInfo4 = new ShardInfo(uuid4, OptionalInt.empty(), ImmutableSet.of("node1"), ImmutableList.of(), 4, 4, 4, 4);

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), true);
        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, ImmutableList.of(shardInfo1, shardInfo2, shardInfo3, shardInfo4), Optional.empty(), 0);

        // delta
        UUID delta1 = UUID.randomUUID();
        ShardInfo deltaInfo1 = new ShardInfo(delta1, OptionalInt.empty(), ImmutableSet.of("node1"), ImmutableList.of(), 1, 1, 1, 1);

        // toReplace
        Map<UUID, DeltaInfoPair> shardMap = ImmutableMap.of(uuid4, new DeltaInfoPair(Optional.empty(), Optional.of(deltaInfo1)));
        transactionId = shardManager.beginTransaction();
        shardManager.replaceDeltaUuids(transactionId, tableId, columns, shardMap, OptionalLong.of(0));

        // check shards on this node1 are correct
        Set<ShardMetadata> shardMetadata = shardManager.getNodeShardsAndDeltas("node1");
        assertEquals(shardMetadata.size(), 3);

        // check index table as well
        Set<BucketShards> shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.all(), true));
        Set<UUID> actualAllUuids = shardNodes.stream()
                .map(BucketShards::getShards)
                .flatMap(Collection::stream)
                .map(ShardNodes::getShardUuid)
                .collect(toSet());
        Set<UUID> expectedAllUuids = ImmutableSet.of(uuid1, uuid2, uuid3, uuid4);
        assertEquals(actualAllUuids, expectedAllUuids);

        // Verify statistics
        Statement statement = dummyHandle.getConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM tables where table_id = " + tableId);
        resultSet.next();
        assertEquals(resultSet.getLong("shard_count"), 4);
        assertEquals(resultSet.getLong("delta_count"), 1);
        assertEquals(resultSet.getLong("row_count"), 9);
        assertEquals(resultSet.getLong("compressed_size"), 11);
        assertEquals(resultSet.getLong("uncompressed_size"), 11);
        resultSet.close();
        statement.close();
    }

    @Test
    public void testReplaceDeltaUuids()
    {
        // node1 shard1 / node2 shard2 / node3 shard3
        // Add delta to shard1
        // Delete shard2
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");
        List<UUID> originalUuids = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        List<ShardInfo> oldShards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(originalUuids.get(0), nodes.get(0)))
                .add(shardInfo(originalUuids.get(1), nodes.get(1)))
                .add(shardInfo(originalUuids.get(2), nodes.get(2)))
                .build();

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), true);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, oldShards, Optional.empty(), 0);

        UUID newDeltaUuid1 = UUID.randomUUID();
        ShardInfo newDeltaShard1 = shardInfo(newDeltaUuid1, nodes.get(0));
        Map<UUID, DeltaInfoPair> shardMap = new HashMap<>();
        shardMap.put(originalUuids.get(0), new DeltaInfoPair(Optional.empty(), Optional.of(newDeltaShard1)));
        shardMap.put(originalUuids.get(1), new DeltaInfoPair(Optional.empty(), Optional.empty()));

        transactionId = shardManager.beginTransaction();
        shardManager.replaceDeltaUuids(transactionId, tableId, columns, shardMap, OptionalLong.of(0));

        // check that delta shard are added in shards table for node1
        Set<ShardMetadata> shardMetadata = shardManager.getNodeShardsAndDeltas(nodes.get(0));
        Map<UUID, Optional<UUID>> actualUuidsMap = shardMetadata.stream().collect(toImmutableMap(ShardMetadata::getShardUuid, ShardMetadata::getDeltaUuid));
        Map<UUID, Optional<UUID>> expectedUuidsMap = ImmutableMap.of(originalUuids.get(0), Optional.of(newDeltaUuid1), newDeltaUuid1, Optional.empty());
        assertEquals(actualUuidsMap, expectedUuidsMap);

        // check that shard are deleted in shards table for node2
        shardMetadata = shardManager.getNodeShardsAndDeltas(nodes.get(1));
        actualUuidsMap = shardMetadata.stream().collect(toImmutableMap(ShardMetadata::getShardUuid, ShardMetadata::getDeltaUuid));
        expectedUuidsMap = ImmutableMap.of();
        assertEquals(actualUuidsMap, expectedUuidsMap);

        // check index table, delta added and shard removed
        Set<BucketShards> shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.all(), true));
        Set<BucketShards> expectedshardNodes = ImmutableSet.of(
                new BucketShards(OptionalInt.empty(), ImmutableSet.of(new ShardNodes(originalUuids.get(0), Optional.of(newDeltaUuid1), ImmutableSet.of(nodes.get(0))))),
                new BucketShards(OptionalInt.empty(), ImmutableSet.of(new ShardNodes(originalUuids.get(2), Optional.empty(), ImmutableSet.of(nodes.get(2))))));
        assertEquals(shardNodes, expectedshardNodes);

        // Verify conflict is handled
        // Try to replace shard1 with newShards without knowing its new delta
        // stimulate the other thread didn't catch the change (actually it's already committed up)
        try {
            transactionId = shardManager.beginTransaction();
            Map<UUID, Optional<UUID>> replaceUuidMap = ImmutableMap.of(originalUuids.get(0), Optional.empty());
            Set<ShardInfo> newShards = ImmutableSet.of(shardInfo(UUID.randomUUID(), nodes.get(0)));
            shardManager.replaceShardUuids(transactionId, tableId, columns, replaceUuidMap, newShards, OptionalLong.of(0), true);
            fail("expected exception");
            // todo check transaction id roll back
            // todo shard change roll back
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
        // Try to delete shard1 with newShards without knowing its new delta
        try {
            transactionId = shardManager.beginTransaction();
            shardManager.replaceDeltaUuids(transactionId, tableId, columns,
                    ImmutableMap.of(originalUuids.get(0), new DeltaInfoPair(Optional.empty(), Optional.empty())), OptionalLong.of(0));
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }

        // node1 shard1 newDelta / node3 shard3
        // replace the newDelta with another new delta
        transactionId = shardManager.beginTransaction();
        UUID anotherNewDeltaUuid1 = UUID.randomUUID();
        shardMap = ImmutableMap.of(originalUuids.get(0), new DeltaInfoPair(Optional.of(newDeltaUuid1), Optional.of(shardInfo(anotherNewDeltaUuid1, nodes.get(0)))));
        shardManager.replaceDeltaUuids(transactionId, tableId, columns, shardMap, OptionalLong.of(0));

        // check that delta shard are added in shards table for node1
        shardMetadata = shardManager.getNodeShardsAndDeltas(nodes.get(0));
        actualUuidsMap = shardMetadata.stream().collect(toImmutableMap(ShardMetadata::getShardUuid, ShardMetadata::getDeltaUuid));
        expectedUuidsMap = ImmutableMap.of(originalUuids.get(0), Optional.of(anotherNewDeltaUuid1), anotherNewDeltaUuid1, Optional.empty());
        assertEquals(actualUuidsMap, expectedUuidsMap);

        // check index table, delta modified
        shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.all(), true));
        expectedshardNodes = ImmutableSet.of(
                new BucketShards(OptionalInt.empty(), ImmutableSet.of(new ShardNodes(originalUuids.get(0), Optional.of(anotherNewDeltaUuid1), ImmutableSet.of(nodes.get(0))))),
                new BucketShards(OptionalInt.empty(), ImmutableSet.of(new ShardNodes(originalUuids.get(2), Optional.empty(), ImmutableSet.of(nodes.get(2))))));
        assertEquals(shardNodes, expectedshardNodes);

        // node1 shard1 anotherNewDelta / node3 shard3
        // rewrite shard1 to shard4
        transactionId = shardManager.beginTransaction();
        UUID uuid4 = UUID.randomUUID();
        Map<UUID, Optional<UUID>> replaceUuidMap = ImmutableMap.of(originalUuids.get(0), Optional.of(anotherNewDeltaUuid1));
        shardManager.replaceShardUuids(transactionId, tableId, columns, replaceUuidMap, ImmutableSet.of(shardInfo(uuid4, nodes.get(0))), OptionalLong.of(0), true);

        // check that new shard are added, old shard and delta are deleted in shards table for node1
        shardMetadata = shardManager.getNodeShardsAndDeltas(nodes.get(0));
        actualUuidsMap = shardMetadata.stream().collect(toImmutableMap(ShardMetadata::getShardUuid, ShardMetadata::getDeltaUuid));
        expectedUuidsMap = ImmutableMap.of(uuid4, Optional.empty());
        assertEquals(actualUuidsMap, expectedUuidsMap);

        // check index table, old shard and delta deleted, new shard added
        shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.all(), true));
        expectedshardNodes = ImmutableSet.of(
                new BucketShards(OptionalInt.empty(), ImmutableSet.of(new ShardNodes(uuid4, Optional.empty(), ImmutableSet.of(nodes.get(0))))),
                new BucketShards(OptionalInt.empty(), ImmutableSet.of(new ShardNodes(originalUuids.get(2), Optional.empty(), ImmutableSet.of(nodes.get(2))))));
        assertEquals(shardNodes, expectedshardNodes);
    }

    @Test
    public void testExternalBatches()
    {
        long tableId = createTable("test");
        Optional<String> externalBatchId = Optional.of("foo");

        List<ShardInfo> shards = ImmutableList.of(shardInfo(UUID.randomUUID(), "node1"));
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, externalBatchId, 0);

        shards = ImmutableList.of(shardInfo(UUID.randomUUID(), "node1"));

        try {
            transactionId = shardManager.beginTransaction();
            shardManager.commitShards(transactionId, tableId, columns, shards, externalBatchId, 0);
            fail("expected external batch exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS.toErrorCode());
        }
    }

    @Test
    public void testBucketAssignments()
    {
        Node node1 = createTestingNode();
        Node node2 = createTestingNode();
        Node node3 = createTestingNode();

        TestingTicker ticker = new TestingTicker();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);
        int bucketCount = 13;
        long distributionId = metadataDao.insertDistribution(null, "test", bucketCount);

        Set<Node> originalNodes = ImmutableSet.of(node1, node2);
        ShardManager shardManager = createShardManager(dbi, () -> originalNodes, ticker);

        shardManager.createBuckets(distributionId, bucketCount);

        List<String> assignments = shardManager.getBucketAssignments(distributionId);
        assertEquals(assignments.size(), bucketCount);
        assertEquals(ImmutableSet.copyOf(assignments), nodeIds(originalNodes));

        Set<Node> newNodes = ImmutableSet.of(node1, node3);
        shardManager = createShardManager(dbi, () -> newNodes, ticker);

        try {
            shardManager.getBucketAssignments(distributionId);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), SERVER_STARTING_UP.toErrorCode());
        }

        ticker.increment(2, DAYS);
        assignments = shardManager.getBucketAssignments(distributionId);
        assertEquals(assignments.size(), bucketCount);
        assertEquals(ImmutableSet.copyOf(assignments), nodeIds(newNodes));

        Set<Node> singleNode = ImmutableSet.of(node1);
        shardManager = createShardManager(dbi, () -> singleNode, ticker);
        ticker.increment(2, DAYS);
        assignments = shardManager.getBucketAssignments(distributionId);
        assertEquals(assignments.size(), bucketCount);
        assertEquals(ImmutableSet.copyOf(assignments), nodeIds(singleNode));
    }

    @Test
    public void testEmptyTable()
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        try (ResultIterator<BucketShards> iterator = shardManager.getShardNodes(tableId, TupleDomain.all(), false)) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testEmptyTableBucketed()
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columns, true, OptionalLong.empty(), false);

        try (ResultIterator<BucketShards> iterator = shardManager.getShardNodesBucketed(tableId, true, ImmutableList.of(), TupleDomain.all(), false)) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void testTemporalColumnTableCreation()
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, TIMESTAMP));
        shardManager.createTable(tableId, columns, false, OptionalLong.of(1), false);

        long tableId2 = createTable("test2");
        List<ColumnInfo> columns2 = ImmutableList.of(new ColumnInfo(1, TIMESTAMP));
        shardManager.createTable(tableId2, columns2, true, OptionalLong.of(1), false);
    }

    @Test
    public void testShardPruning()
    {
        ShardInfo shard1 = shardInfo(
                UUID.randomUUID(),
                "node1",
                ImmutableList.<ColumnStats>builder()
                        .add(new ColumnStats(1, 5, 10))
                        .add(new ColumnStats(2, -20.0, 20.0))
                        .add(new ColumnStats(3, date(2013, 5, 11), date(2013, 6, 13)))
                        .add(new ColumnStats(4, timestamp(2013, 5, 11, 4, 5, 6), timestamp(2013, 6, 13, 7, 8, 9)))
                        .add(new ColumnStats(5, "hello", "world"))
                        .add(new ColumnStats(6, false, true))
                        .build());

        ShardInfo shard2 = shardInfo(
                UUID.randomUUID(),
                "node2",
                ImmutableList.<ColumnStats>builder()
                        .add(new ColumnStats(1, 2, 8))
                        .add(new ColumnStats(2, null, 50.0))
                        .add(new ColumnStats(3, date(2012, 1, 1), date(2012, 12, 31)))
                        .add(new ColumnStats(4, timestamp(2012, 1, 1, 2, 3, 4), timestamp(2012, 12, 31, 5, 6, 7)))
                        .add(new ColumnStats(5, "cat", "dog"))
                        .add(new ColumnStats(6, true, true))
                        .build());

        ShardInfo shard3 = shardInfo(
                UUID.randomUUID(),
                "node3",
                ImmutableList.<ColumnStats>builder()
                        .add(new ColumnStats(1, 15, 20))
                        .add(new ColumnStats(2, null, null))
                        .add(new ColumnStats(3, date(2013, 4, 1), date(2013, 6, 1)))
                        .add(new ColumnStats(4, timestamp(2013, 4, 1, 8, 7, 6), timestamp(2013, 6, 1, 6, 5, 4)))
                        .add(new ColumnStats(5, "grape", "orange"))
                        .add(new ColumnStats(6, false, false))
                        .build());

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shard1)
                .add(shard2)
                .add(shard3)
                .build();

        List<ColumnInfo> columns = ImmutableList.<ColumnInfo>builder()
                .add(new ColumnInfo(1, BIGINT))
                .add(new ColumnInfo(2, DOUBLE))
                .add(new ColumnInfo(3, DATE))
                .add(new ColumnInfo(4, TIMESTAMP))
                .add(new ColumnInfo(5, createVarcharType(10)))
                .add(new ColumnInfo(6, BOOLEAN))
                .add(new ColumnInfo(7, VARBINARY))
                .build();

        RaptorColumnHandle c1 = new RaptorColumnHandle("raptor", "c1", 1, BIGINT);
        RaptorColumnHandle c2 = new RaptorColumnHandle("raptor", "c2", 2, DOUBLE);
        RaptorColumnHandle c3 = new RaptorColumnHandle("raptor", "c3", 3, DATE);
        RaptorColumnHandle c4 = new RaptorColumnHandle("raptor", "c4", 4, TIMESTAMP);
        RaptorColumnHandle c5 = new RaptorColumnHandle("raptor", "c5", 5, createVarcharType(10));
        RaptorColumnHandle c6 = new RaptorColumnHandle("raptor", "c6", 6, BOOLEAN);

        long tableId = createTable("test");
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        shardAssertion(tableId).expected(shards);

        shardAssertion(tableId).equal(c1, BIGINT, 3L).expected(shard2);
        shardAssertion(tableId).equal(c1, BIGINT, 8L).expected(shard1, shard2);
        shardAssertion(tableId).equal(c1, BIGINT, 9L).expected(shard1);
        shardAssertion(tableId).equal(c1, BIGINT, 13L).expected();
        shardAssertion(tableId).between(c1, BIGINT, 8L, 14L).expected(shard1, shard2);
        shardAssertion(tableId).between(c1, BIGINT, 8L, 15L).expected(shards);
        shardAssertion(tableId).between(c1, BIGINT, 8L, 16L).expected(shards);
        shardAssertion(tableId).between(c1, BIGINT, 12L, 14L).expected();
        shardAssertion(tableId).between(c1, BIGINT, 5L, 10L).expected(shard1, shard2);
        shardAssertion(tableId).between(c1, BIGINT, 16L, 18L).expected(shard3);
        shardAssertion(tableId).between(c1, BIGINT, 1L, 25L).expected(shards);
        shardAssertion(tableId).between(c1, BIGINT, 4L, 12L).expected(shard1, shard2);
        shardAssertion(tableId).range(c1, lessThan(BIGINT, 5L)).expected(shard1, shard2);
        shardAssertion(tableId).range(c1, lessThan(BIGINT, 4L)).expected(shard2);
        shardAssertion(tableId).range(c1, lessThan(BIGINT, 11L)).expected(shard1, shard2);
        shardAssertion(tableId).range(c1, lessThan(BIGINT, 25L)).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(BIGINT, 1L)).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(BIGINT, 8L)).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(BIGINT, 9L)).expected(shard1, shard3);

        shardAssertion(tableId)
                .between(c1, BIGINT, -25L, 25L)
                .between(c2, DOUBLE, -1000.0, 1000.0)
                .between(c3, BIGINT, 0L, 50000L)
                .between(c4, TIMESTAMP, 0L, timestamp(2015, 1, 2, 3, 4, 5))
                .between(c5, createVarcharType(10), utf8Slice("a"), utf8Slice("zzzzz"))
                .between(c6, BOOLEAN, false, true)
                .expected(shards);

        shardAssertion(tableId)
                .between(c1, BIGINT, 4L, 12L)
                .between(c3, DATE, date(2013, 3, 3), date(2013, 5, 25))
                .expected(shard1);

        shardAssertion(tableId).equal(c2, DOUBLE, 25.0).expected(shard2, shard3);
        shardAssertion(tableId).equal(c2, DOUBLE, 50.1).expected(shard3);

        shardAssertion(tableId).equal(c3, DATE, date(2013, 5, 12)).expected(shard1, shard3);

        shardAssertion(tableId).range(c4, greaterThan(TIMESTAMP, timestamp(2013, 1, 1, 0, 0, 0))).expected(shard1, shard3);

        shardAssertion(tableId).between(c5, createVarcharType(10), utf8Slice("cow"), utf8Slice("milk")).expected(shards);
        shardAssertion(tableId).equal(c5, createVarcharType(10), utf8Slice("fruit")).expected();
        shardAssertion(tableId).equal(c5, createVarcharType(10), utf8Slice("pear")).expected(shard1);
        shardAssertion(tableId).equal(c5, createVarcharType(10), utf8Slice("cat")).expected(shard2);
        shardAssertion(tableId).range(c5, greaterThan(createVarcharType(10), utf8Slice("gum"))).expected(shard1, shard3);
        shardAssertion(tableId).range(c5, lessThan(createVarcharType(10), utf8Slice("air"))).expected();

        shardAssertion(tableId).equal(c6, BOOLEAN, true).expected(shard1, shard2);
        shardAssertion(tableId).equal(c6, BOOLEAN, false).expected(shard1, shard3);
        shardAssertion(tableId).range(c6, greaterThanOrEqual(BOOLEAN, false)).expected(shards);
        shardAssertion(tableId).range(c6, lessThan(BOOLEAN, true)).expected(shards);
        shardAssertion(tableId).range(c6, lessThan(BOOLEAN, false)).expected(shard1, shard3);

        // Test multiple ranges
        shardAssertion(tableId)
                .domain(c1, createDomain(lessThan(BIGINT, 0L), greaterThan(BIGINT, 25L)))
                .expected();

        shardAssertion(tableId)
                .domain(c1, createDomain(range(BIGINT, 3L, true, 4L, true), range(BIGINT, 16L, true, 18L, true)))
                .expected(shard2, shard3);

        shardAssertion(tableId)
                .domain(c5, createDomain(
                        range(createVarcharType(10), utf8Slice("gum"), true, utf8Slice("happy"), true),
                        range(createVarcharType(10), utf8Slice("pear"), true, utf8Slice("wall"), true)))
                .expected(shard1, shard3);

        shardAssertion(tableId)
                .domain(c1, createDomain(range(BIGINT, 3L, true, 4L, true), range(BIGINT, 16L, true, 18L, true)))
                .domain(c5, createDomain(
                        range(createVarcharType(10), utf8Slice("gum"), true, utf8Slice("happy"), true),
                        range(createVarcharType(10), utf8Slice("pear"), true, utf8Slice("wall"), true)))
                .expected(shard3);
    }

    @Test
    public void testShardPruningTruncatedValues()
    {
        String prefix = repeat("x", MAX_BINARY_INDEX_SIZE);

        ColumnStats stats = new ColumnStats(1, prefix + "a", prefix + "z");
        ShardInfo shard = shardInfo(UUID.randomUUID(), "node", ImmutableList.of(stats));

        List<ShardInfo> shards = ImmutableList.of(shard);

        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, createVarcharType(10)));
        RaptorColumnHandle c1 = new RaptorColumnHandle("raptor", "c1", 1, createVarcharType(10));

        long tableId = createTable("test");
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        shardAssertion(tableId).expected(shards);
        shardAssertion(tableId).equal(c1, createVarcharType(10), utf8Slice(prefix)).expected(shards);
        shardAssertion(tableId).equal(c1, createVarcharType(10), utf8Slice(prefix + "c")).expected(shards);
        shardAssertion(tableId).range(c1, lessThan(createVarcharType(10), utf8Slice(prefix + "c"))).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(createVarcharType(10), utf8Slice(prefix + "zzz"))).expected(shards);

        shardAssertion(tableId).between(c1, createVarcharType(10), utf8Slice("w"), utf8Slice("y")).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(createVarcharType(10), utf8Slice("x"))).expected(shards);

        shardAssertion(tableId).between(c1, createVarcharType(10), utf8Slice("x"), utf8Slice("x")).expected();
        shardAssertion(tableId).range(c1, lessThan(createVarcharType(10), utf8Slice("w"))).expected();
        shardAssertion(tableId).range(c1, lessThan(createVarcharType(10), utf8Slice("x"))).expected();
        shardAssertion(tableId).range(c1, greaterThan(createVarcharType(10), utf8Slice("y"))).expected();

        Slice shorter = utf8Slice(prefix.substring(0, prefix.length() - 1));
        shardAssertion(tableId).equal(c1, createVarcharType(10), shorter).expected();
        shardAssertion(tableId).range(c1, lessThan(createVarcharType(10), shorter)).expected();
        shardAssertion(tableId).range(c1, greaterThan(createVarcharType(10), shorter)).expected(shards);
    }

    @Test
    public void testShardPruningNoStats()
    {
        ShardInfo shard = shardInfo(UUID.randomUUID(), "node");
        List<ShardInfo> shards = ImmutableList.of(shard);

        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        RaptorColumnHandle c1 = new RaptorColumnHandle("raptor", "c1", 1, BIGINT);

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        shardAssertion(tableId).expected(shards);
        shardAssertion(tableId).equal(c1, BIGINT, 3L).expected(shards);
    }

    @Test
    public void testAddNewColumn()
            throws Exception
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);
        int before = columnCount(tableId);

        ColumnInfo newColumn = new ColumnInfo(2, BIGINT);
        shardManager.addColumn(tableId, newColumn);
        int after = columnCount(tableId);
        // should be 2 more: min and max columns
        assertEquals(after, before + 2);
    }

    @Test
    public void testAddDuplicateColumn()
            throws Exception
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);
        int before = columnCount(tableId);

        shardManager.addColumn(tableId, columns.get(0));
        int after = columnCount(tableId);
        // no error, no columns added
        assertEquals(after, before);
    }

    @Test
    public void testMaintenanceBlocked()
    {
        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        Set<UUID> oldShards = ImmutableSet.of(UUID.randomUUID());

        dbi.onDemand(MetadataDao.class).blockMaintenance(tableId);

        long transactionId = shardManager.beginTransaction();
        try {
            Map<UUID, Optional<UUID>> oldShardMap = oldShards.stream().collect(toImmutableMap(Function.identity(), uuid -> Optional.empty()));
            shardManager.replaceShardUuids(transactionId, tableId, columns, oldShardMap, ImmutableSet.of(), OptionalLong.empty(), false);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
            assertEquals(e.getMessage(), "Maintenance is blocked for table");
        }
    }

    private Set<ShardNodes> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> predicate)
    {
        try (ResultIterator<BucketShards> iterator = shardManager.getShardNodes(tableId, predicate, false)) {
            return ImmutableSet.copyOf(concat(transform(iterator, i -> i.getShards().iterator())));
        }
    }

    private long createTable(String name)
    {
        return dbi.onDemand(MetadataDao.class).insertTable("test", name, false, false, null, 0, false);
    }

    public static ShardInfo shardInfo(UUID shardUuid, String nodeIdentifier)
    {
        return shardInfo(shardUuid, nodeIdentifier, ImmutableList.of());
    }

    public static ShardInfo shardInfo(UUID shardUuid, String nodeId, List<ColumnStats> columnStats)
    {
        return new ShardInfo(shardUuid, OptionalInt.empty(), ImmutableSet.of(nodeId), columnStats, 0, 0, 0, 0);
    }

    private static Set<ShardNodes> toShardNodes(List<ShardInfo> shards)
    {
        return shards.stream()
                .map(shard -> new ShardNodes(shard.getShardUuid(), Optional.empty(), shard.getNodeIdentifiers()))
                .collect(toSet());
    }

    public static ShardManager createShardManager(IDBI dbi)
    {
        return createShardManager(dbi, ImmutableSet::of, systemTicker());
    }

    public static ShardManager createShardManager(IDBI dbi, NodeSupplier nodeSupplier)
    {
        return createShardManager(dbi, nodeSupplier, systemTicker());
    }

    public static ShardManager createShardManager(IDBI dbi, NodeSupplier nodeSupplier, Ticker ticker)
    {
        DaoSupplier<ShardDao> shardDaoSupplier = new DaoSupplier<>(dbi, H2ShardDao.class);
        AssignmentLimiter assignmentLimiter = new AssignmentLimiter(nodeSupplier, ticker, new MetadataConfig());
        return new DatabaseShardManager(dbi, shardDaoSupplier, nodeSupplier, assignmentLimiter, ticker, new Duration(1, DAYS));
    }

    private static Domain createDomain(Range first, Range... ranges)
    {
        return Domain.create(ValueSet.ofRanges(first, ranges), false);
    }

    private ShardAssertion shardAssertion(long tableId)
    {
        return new ShardAssertion(tableId);
    }

    private class ShardAssertion
    {
        private final Map<RaptorColumnHandle, Domain> domains = new HashMap<>();
        private final long tableId;

        public ShardAssertion(long tableId)
        {
            this.tableId = tableId;
        }

        public ShardAssertion domain(RaptorColumnHandle column, Domain domain)
        {
            domains.put(column, domain);
            return this;
        }

        public ShardAssertion range(RaptorColumnHandle column, Range range)
        {
            return domain(column, createDomain(range));
        }

        public ShardAssertion equal(RaptorColumnHandle column, Type type, Object value)
        {
            return domain(column, Domain.singleValue(type, value));
        }

        public ShardAssertion between(RaptorColumnHandle column, Type type, Object low, Object high)
        {
            return range(column, Range.range(type, low, true, high, true));
        }

        public void expected(ShardInfo... shards)
        {
            expected(ImmutableList.copyOf(shards));
        }

        public void expected(List<ShardInfo> shards)
        {
            TupleDomain<RaptorColumnHandle> predicate = TupleDomain.withColumnDomains(domains);
            Set<ShardNodes> actual = getShardNodes(tableId, predicate);
            assertEquals(actual, toShardNodes(shards));
        }
    }

    private static long date(int year, int month, int day)
    {
        return LocalDate.of(year, month, day).toEpochDay();
    }

    private static long timestamp(int year, int month, int day, int hour, int minute, int second)
    {
        return ZonedDateTime.of(year, month, day, hour, minute, second, 0, UTC).toInstant().toEpochMilli();
    }

    private static Set<String> nodeIds(Collection<Node> nodes)
    {
        return nodes.stream().map(Node::getNodeIdentifier).collect(toSet());
    }

    private static Node createTestingNode()
    {
        return new InternalNode(UUID.randomUUID().toString(), URI.create("http://test"), NodeVersion.UNKNOWN, false);
    }

    private int columnCount(long tableId)
            throws SQLException
    {
        try (Statement statement = dummyHandle.getConnection().createStatement()) {
            try (ResultSet rs = statement.executeQuery(format("SELECT * FROM %s LIMIT 0", shardIndexTable(tableId)))) {
                return rs.getMetaData().getColumnCount();
            }
        }
    }
}
