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

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.util.CloseableIterator;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import io.airlift.slice.Slice;
import io.airlift.testing.FileUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.spi.Range.greaterThan;
import static com.facebook.presto.spi.Range.greaterThanOrEqual;
import static com.facebook.presto.spi.Range.lessThan;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestDatabaseShardManager
{
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;

    @BeforeMethod
    public void setup()
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
    public void testCommit()
            throws Exception
    {
        long tableId = 1;

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node1"))
                .add(shardInfo(UUID.randomUUID(), "node2"))
                .build();

        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shards, Optional.empty());

        Set<ShardNodes> actual = getShardNodes(tableId, TupleDomain.all());
        assertEquals(actual, toShardNodes(shards));
    }

    @Test
    public void testAssignShard()
    {
        long tableId = 1;
        UUID shard = UUID.randomUUID();
        List<ShardInfo> shardNodes = ImmutableList.of(shardInfo(shard, "node1"));
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shardNodes, Optional.empty());

        ShardNodes actual = getOnlyElement(getShardNodes(tableId, TupleDomain.all()));
        assertEquals(actual, new ShardNodes(shard, ImmutableSet.of("node1")));

        shardManager.assignShard(tableId, shard, "node2");

        actual = getOnlyElement(getShardNodes(tableId, TupleDomain.all()));
        assertEquals(actual, new ShardNodes(shard, ImmutableSet.of("node1", "node2")));
    }

    @Test
    public void testGetNodeTableShards()
            throws Exception
    {
        long tableId = 1;
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");

        ImmutableList.Builder<ShardInfo> inputShards = ImmutableList.builder();
        Multimap<String, UUID> nodeShardMap = HashMultimap.create();
        for (String node : nodes) {
            UUID uuid = UUID.randomUUID();
            nodeShardMap.put(node, uuid);
            inputShards.add(shardInfo(uuid, node));
        }

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, inputShards.build(), Optional.empty());

        for (String node : nodes) {
            Set<ShardMetadata> shardMetadata = shardManager.getNodeTableShards(node, tableId);
            Set<UUID> expectedUuids = ImmutableSet.copyOf(nodeShardMap.get(node));
            Set<UUID> actualUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());
            assertEquals(actualUuids, expectedUuids);
        }

        shardManager.dropTableShards(tableId);
        for (String node : nodes) {
            Set<ShardMetadata> shardMetadata = shardManager.getNodeTableShards(node, tableId);
            assertEquals(shardMetadata.size(), 0);
        }
    }

    @Test
    public void testReplaceShards()
            throws Exception
    {
        long tableId = 1;
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        List<String> nodes = ImmutableList.of("node1", "node2", "node3");
        List<UUID> originalUuids = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        List<ShardInfo> oldShards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(originalUuids.get(0), nodes.get(0)))
                .add(shardInfo(originalUuids.get(1), nodes.get(1)))
                .add(shardInfo(originalUuids.get(2), nodes.get(2)))
                .build();

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, oldShards, Optional.empty());

        List<UUID> expectedUuids = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID());
        List<ShardInfo> newShards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(expectedUuids.get(0), nodes.get(0)))
                .add(shardInfo(expectedUuids.get(1), nodes.get(0)))
                .build();

        Set<ShardMetadata> shardMetadata = shardManager.getNodeTableShards(nodes.get(0), tableId);
        Set<Long> shardIds = shardMetadata.stream().map(ShardMetadata::getShardId).collect(toSet());
        Set<UUID> replacedUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());

        shardManager.replaceShards(tableId, columns, shardIds, newShards);

        shardMetadata = shardManager.getNodeTableShards(nodes.get(0), tableId);
        Set<UUID> actualUuids = shardMetadata.stream().map(ShardMetadata::getShardUuid).collect(toSet());
        assertEquals(actualUuids, ImmutableSet.copyOf(expectedUuids));

        // Compute expected all uuids for this table
        Set<UUID> expectedAllUuids = new HashSet<>(originalUuids);
        expectedAllUuids.removeAll(replacedUuids);
        expectedAllUuids.addAll(expectedUuids);

        // check that shards are replaced in index table as well
        Set<ShardNodes> shardNodes = ImmutableSet.copyOf(shardManager.getShardNodes(tableId, TupleDomain.<RaptorColumnHandle>all()));
        Set<UUID> actualAllUuids = shardNodes.stream().map(ShardNodes::getShardUuid).collect(toSet());
        assertEquals(actualAllUuids, expectedAllUuids);

        // verify that conflicting updates are handled
        newShards = ImmutableList.of(shardInfo(UUID.randomUUID(), nodes.get(0)));
        try {
            shardManager.replaceShards(tableId, columns, shardIds, newShards);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), TRANSACTION_CONFLICT.toErrorCode());
        }
    }

    @Test
    public void testExternalBatches()
            throws Exception
    {
        long tableId = 1;
        Optional<String> externalBatchId = Optional.of("foo");

        List<ShardInfo> shards = ImmutableList.of(shardInfo(UUID.randomUUID(), "node1"));
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shards, externalBatchId);

        shards = ImmutableList.of(shardInfo(UUID.randomUUID(), "node1"));

        try {
            shardManager.commitShards(tableId, columns, shards, externalBatchId);
            fail("expected external batch exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS.toErrorCode());
        }
    }

    @Test
    public void testShardPruning()
            throws Exception
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
                .add(new ColumnInfo(5, VARCHAR))
                .add(new ColumnInfo(6, BOOLEAN))
                .add(new ColumnInfo(7, VARBINARY))
                .build();

        RaptorColumnHandle c1 = new RaptorColumnHandle("raptor", "c1", 1, BIGINT);
        RaptorColumnHandle c2 = new RaptorColumnHandle("raptor", "c2", 2, DOUBLE);
        RaptorColumnHandle c3 = new RaptorColumnHandle("raptor", "c3", 3, DATE);
        RaptorColumnHandle c4 = new RaptorColumnHandle("raptor", "c4", 4, TIMESTAMP);
        RaptorColumnHandle c5 = new RaptorColumnHandle("raptor", "c5", 5, VARCHAR);
        RaptorColumnHandle c6 = new RaptorColumnHandle("raptor", "c6", 6, BOOLEAN);

        long tableId = 1;
        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shards, Optional.empty());

        shardAssertion(tableId).expected(shards);

        shardAssertion(tableId).equal(c1, 3).expected(shard2);
        shardAssertion(tableId).equal(c1, 8).expected(shard1, shard2);
        shardAssertion(tableId).equal(c1, 9).expected(shard1);
        shardAssertion(tableId).equal(c1, 13).expected();
        shardAssertion(tableId).between(c1, 8, 14).expected(shard1, shard2);
        shardAssertion(tableId).between(c1, 8, 15).expected(shards);
        shardAssertion(tableId).between(c1, 8, 16).expected(shards);
        shardAssertion(tableId).between(c1, 12, 14).expected();
        shardAssertion(tableId).between(c1, 5, 10).expected(shard1, shard2);
        shardAssertion(tableId).between(c1, 16, 18).expected(shard3);
        shardAssertion(tableId).between(c1, 1, 25).expected(shards);
        shardAssertion(tableId).between(c1, 4, 12).expected(shard1, shard2);
        shardAssertion(tableId).range(c1, lessThan(5)).expected(shard1, shard2);
        shardAssertion(tableId).range(c1, lessThan(4)).expected(shard2);
        shardAssertion(tableId).range(c1, lessThan(11)).expected(shard1, shard2);
        shardAssertion(tableId).range(c1, lessThan(25)).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(1)).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(8)).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(9)).expected(shard1, shard3);

        shardAssertion(tableId)
                .between(c1, -25, 25)
                .between(c2, -1000.0, 1000.0)
                .between(c3, 0, 50000)
                .between(c4, 0L, timestamp(2015, 1, 2, 3, 4, 5))
                .between(c5, utf8Slice("a"), utf8Slice("zzzzz"))
                .between(c6, false, true)
                .expected(shards);

        shardAssertion(tableId)
                .between(c1, 4, 12)
                .between(c3, date(2013, 3, 3), date(2013, 5, 25))
                .expected(shard1);

        shardAssertion(tableId).equal(c2, 25.0).expected(shard2, shard3);
        shardAssertion(tableId).equal(c2, 50.1).expected(shard3);

        shardAssertion(tableId).equal(c3, date(2013, 5, 12)).expected(shard1, shard3);

        shardAssertion(tableId).range(c4, greaterThan(timestamp(2013, 1, 1, 0, 0, 0))).expected(shard1, shard3);

        shardAssertion(tableId).between(c5, utf8Slice("cow"), utf8Slice("milk")).expected(shards);
        shardAssertion(tableId).equal(c5, utf8Slice("fruit")).expected();
        shardAssertion(tableId).equal(c5, utf8Slice("pear")).expected(shard1);
        shardAssertion(tableId).equal(c5, utf8Slice("cat")).expected(shard2);
        shardAssertion(tableId).range(c5, greaterThan(utf8Slice("gum"))).expected(shard1, shard3);
        shardAssertion(tableId).range(c5, lessThan(utf8Slice("air"))).expected();

        shardAssertion(tableId).equal(c6, true).expected(shard1, shard2);
        shardAssertion(tableId).equal(c6, false).expected(shard1, shard3);
        shardAssertion(tableId).range(c6, greaterThanOrEqual(false)).expected(shards);
        shardAssertion(tableId).range(c6, lessThan(true)).expected(shards);
        shardAssertion(tableId).range(c6, lessThan(false)).expected(shard1, shard3);

        // TODO: support multiple ranges
        shardAssertion(tableId)
                .domain(c1, createDomain(lessThan(0), greaterThan(25)))
                .expected(shards);
    }

    @Test
    public void testShardPruningTruncatedValues()
            throws Exception
    {
        String prefix = repeat("x", MAX_BINARY_INDEX_SIZE);

        ColumnStats stats = new ColumnStats(1, prefix + "a", prefix + "z");
        ShardInfo shard = shardInfo(UUID.randomUUID(), "node", ImmutableList.of(stats));

        List<ShardInfo> shards = ImmutableList.of(shard);

        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, VARCHAR));
        RaptorColumnHandle c1 = new RaptorColumnHandle("raptor", "c1", 1, VARCHAR);

        long tableId = 1;
        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shards, Optional.empty());

        shardAssertion(tableId).expected(shards);
        shardAssertion(tableId).equal(c1, utf8Slice(prefix)).expected(shards);
        shardAssertion(tableId).equal(c1, utf8Slice(prefix + "c")).expected(shards);
        shardAssertion(tableId).range(c1, lessThan(utf8Slice(prefix + "c"))).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(utf8Slice(prefix + "zzz"))).expected(shards);

        shardAssertion(tableId).between(c1, utf8Slice("w"), utf8Slice("y")).expected(shards);
        shardAssertion(tableId).range(c1, greaterThan(utf8Slice("x"))).expected(shards);

        shardAssertion(tableId).between(c1, utf8Slice("x"), utf8Slice("x")).expected();
        shardAssertion(tableId).range(c1, lessThan(utf8Slice("w"))).expected();
        shardAssertion(tableId).range(c1, lessThan(utf8Slice("x"))).expected();
        shardAssertion(tableId).range(c1, greaterThan(utf8Slice("y"))).expected();

        Slice shorter = utf8Slice(prefix.substring(0, prefix.length() - 1));
        shardAssertion(tableId).equal(c1, shorter).expected();
        shardAssertion(tableId).range(c1, lessThan(shorter)).expected();
        shardAssertion(tableId).range(c1, greaterThan(shorter)).expected(shards);
    }

    @Test
    public void testShardPruningNoStats()
            throws Exception
    {
        ShardInfo shard = shardInfo(UUID.randomUUID(), "node");
        List<ShardInfo> shards = ImmutableList.of(shard);

        long tableId = 1;
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));
        RaptorColumnHandle c1 = new RaptorColumnHandle("raptor", "c1", 1, BIGINT);

        shardManager.createTable(tableId, columns);
        shardManager.commitShards(tableId, columns, shards, Optional.empty());

        shardAssertion(tableId).expected(shards);
        shardAssertion(tableId).equal(c1, 3).expected(shards);
    }

    private Set<ShardNodes> getShardNodes(long tableId, TupleDomain<RaptorColumnHandle> predicate)
    {
        try (CloseableIterator<ShardNodes> iterator = shardManager.getShardNodes(tableId, predicate)) {
            return ImmutableSet.copyOf(iterator);
        }
    }

    static ShardInfo shardInfo(UUID shardUuid, String nodeIdentifier)
    {
        return shardInfo(shardUuid, nodeIdentifier, ImmutableList.of());
    }

    private static ShardInfo shardInfo(UUID shardUuid, String nodeId, List<ColumnStats> columnStats)
    {
        return new ShardInfo(shardUuid, ImmutableSet.of(nodeId), columnStats, 0, 0, 0);
    }

    private static Set<ShardNodes> toShardNodes(List<ShardInfo> shards)
    {
        return shards.stream()
                .map(shard -> new ShardNodes(shard.getShardUuid(), shard.getNodeIdentifiers()))
                .collect(toSet());
    }

    private static Domain createDomain(Range first, Range... ranges)
    {
        return Domain.create(SortedRangeSet.of(first, ranges), false);
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

        public ShardAssertion equal(RaptorColumnHandle column, Comparable<?> value)
        {
            return domain(column, Domain.singleValue(value));
        }

        public <T> ShardAssertion between(RaptorColumnHandle column, Comparable<T> low, Comparable<T> high)
        {
            return range(column, Range.range(low, true, high, true));
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
}
