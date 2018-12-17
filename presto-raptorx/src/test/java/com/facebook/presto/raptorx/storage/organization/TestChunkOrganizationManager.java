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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DatabaseChunkManager;
import com.facebook.presto.raptorx.metadata.MasterReaderDao;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.raptorx.storage.organization.ChunkOrganizationManager.createOrganizationSets;
import static com.facebook.presto.raptorx.storage.organization.TestChunkOrganizer.createChunkOrganizer;
import static com.facebook.presto.raptorx.storage.organization.TestChunkOrganizerUtil.column;
import static com.facebook.presto.raptorx.storage.organization.TestCompactionSetCreator.extractIndexes;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestChunkOrganizationManager
{
    private TestingDatabase database;
    private Metadata metadata;
    private NodeIdCache nodeIdCache;
    private TransactionWriter transactionWriter;
    private MasterReaderDao masterReaderDao;
    private ChunkManager chunkManager;

    private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
            new ColumnInfo(1, "1", BIGINT, Optional.empty(), 1, OptionalInt.empty(), OptionalInt.empty()));

    private static final TableInfo tableInfo = new TableInfo(1L, "1", 10, 1, OptionalLong.empty(), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), COLUMNS);
    private static final TableInfo temporalTableInfo = new TableInfo(2L, "2", 10, 1, OptionalLong.of(1L), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), COLUMNS);

    private static final List<Type> types = ImmutableList.of(BIGINT, VARCHAR, DATE, TIMESTAMP);
    private static final TemporalFunction TEMPORAL_FUNCTION = new TemporalFunction(UTC);

    long distributionId;

    @BeforeMethod
    public void setup()
    {
        database = new TestingDatabase();
        new SchemaCreator(database).create();
        TestingEnvironment environment = new TestingEnvironment(database);
        metadata = environment.getMetadata();
        masterReaderDao = createJdbi(database.getMasterConnection()).onDemand(MasterReaderDao.class);
        transactionWriter = environment.getTransactionWriter();
        nodeIdCache = environment.getNodeIdCache();
        distributionId = metadata.createDistribution(Optional.empty(), ImmutableList.of(), nCopies(5, 1L));
        chunkManager = new DatabaseChunkManager(nodeIdCache, database, environment.getTypeManager());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void testOrganizationEligibleTables()
    {
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        long tableId1 = metadata.nextTableId();
        transaction.createTable(tableId1, 1, "1", distributionId, OptionalLong.of(20), CompressionType.ZSTD, System.currentTimeMillis(), Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(10, "orderkey", BIGINT, 1, OptionalInt.of(1)))
                        .add(column(20, "orderdate", DATE, 2, OptionalInt.empty()))
                        .build(), true);
        long tableId2 = metadata.nextTableId();
        transaction.createTable(tableId2, 1, "2", distributionId, OptionalLong.empty(), CompressionType.ZSTD, System.currentTimeMillis(), Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(10, "orderkey", BIGINT, 1, OptionalInt.empty()))
                        .build(), true);
        long tableId3 = metadata.nextTableId();
        transaction.createTable(tableId3, 1, "3", distributionId, OptionalLong.empty(), CompressionType.ZSTD, System.currentTimeMillis(), Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(10, "orderkey", BIGINT, 1, OptionalInt.of(1)))
                        .build(), true);
        transactionWriter.write(transaction.getActions(), OptionalLong.empty());
        assertEquals(masterReaderDao.getOrganizationEligibleTables(), ImmutableSet.of(tableId3));
    }

    @Test
    public void testTableDiscovery()
            throws Exception
    {
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        long tableId1 = metadata.nextTableId();
        transaction.createTable(tableId1, 1, "3", distributionId, OptionalLong.empty(), CompressionType.ZSTD, System.currentTimeMillis(), Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(10, "orderkey", BIGINT, 1, OptionalInt.of(1)))
                        .build(), true);
        transactionWriter.write(transaction.getActions(), OptionalLong.empty());

        long intervalMillis = 100;
        ChunkOrganizationManager organizationManager = createChunkOrganizationManager(intervalMillis);

        // initializes tables
        Set<Long> actual = organizationManager.discoverAndInitializeTablesToOrganize();
        assertEquals(actual, ImmutableSet.of(tableId1));

        // update the start times and test that the tables are discovered after interval seconds
        long updateTime = System.currentTimeMillis();
        ChunkOrganizerDao organizerDao = createJdbi(database.getMasterConnection()).onDemand(ChunkOrganizerDao.class);
        organizerDao.updateLastStartTime(1, tableId1, updateTime);
        organizerDao.updateLastStartTime(1, tableId1, updateTime);

        // wait for some time (interval time) for the tables to be eligible for organization
        long start = System.nanoTime();
        while (organizationManager.discoverAndInitializeTablesToOrganize().isEmpty() &&
                nanosSince(start).toMillis() < intervalMillis + 1000) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(organizationManager.discoverAndInitializeTablesToOrganize(), ImmutableSet.of(tableId1));
    }

    @Test
    public void testSimple()
    {
        long timestamp = 1L;
        int day = 1;

        List<ChunkIndexInfo> shards = ImmutableList.of(
                shardWithSortRange(1, ChunkRange.of(new Tuple(types, 5L, "hello", day, timestamp), new Tuple(types, 10L, "hello", day, timestamp))),
                shardWithSortRange(1, ChunkRange.of(new Tuple(types, 7L, "hello", day, timestamp), new Tuple(types, 10L, "hello", day, timestamp))),
                shardWithSortRange(1, ChunkRange.of(new Tuple(types, 6L, "hello", day, timestamp), new Tuple(types, 9L, "hello", day, timestamp))),
                shardWithSortRange(1, ChunkRange.of(new Tuple(types, 1L, "hello", day, timestamp), new Tuple(types, 5L, "hello", day, timestamp))));
        Set<OrganizationSet> actual = createOrganizationSets(TEMPORAL_FUNCTION, tableInfo, shards);

        assertEquals(actual.size(), 1);
        // Shards 0, 1 and 2 are overlapping, so we should get an organization set with these shards
        assertEquals(getOnlyElement(actual).getChunkIds(), extractIndexes(shards, 0, 1, 2));
    }

    @Test
    public void testSimpleTemporal()
    {
        List<Type> temporalType = ImmutableList.of(DATE);
        List<Type> types = ImmutableList.of(BIGINT);

        int day1 = 1;
        int day2 = 2;
        int day4 = 4;
        int day5 = 5;

        List<ChunkIndexInfo> shards = ImmutableList.of(
                shardWithTemporalRange(1, ChunkRange.of(new Tuple(types, 5L), new Tuple(types, 10L)), ChunkRange.of(new Tuple(temporalType, day1), new Tuple(temporalType, day2))),
                shardWithTemporalRange(1, ChunkRange.of(new Tuple(types, 7L), new Tuple(types, 10L)), ChunkRange.of(new Tuple(temporalType, day4), new Tuple(temporalType, day5))),
                shardWithTemporalRange(1, ChunkRange.of(new Tuple(types, 6L), new Tuple(types, 9L)), ChunkRange.of(new Tuple(temporalType, day1), new Tuple(temporalType, day2))),
                shardWithTemporalRange(1, ChunkRange.of(new Tuple(types, 4L), new Tuple(types, 8L)), ChunkRange.of(new Tuple(temporalType, day4), new Tuple(temporalType, day5))));

        Set<OrganizationSet> organizationSets = createOrganizationSets(TEMPORAL_FUNCTION, temporalTableInfo, shards);
        Set<Set<Long>> actual = organizationSets.stream()
                .map(OrganizationSet::getChunkIds)
                .collect(toSet());

        // expect 2 organization sets, of overlapping shards (0, 2) and (1, 3)
        assertEquals(organizationSets.size(), 2);
        assertEquals(actual, ImmutableSet.of(extractIndexes(shards, 0, 2), extractIndexes(shards, 1, 3)));
    }

    private static ChunkIndexInfo shardWithSortRange(int bucketNumber, ChunkRange sortRange)
    {
        return new ChunkIndexInfo(
                1,
                bucketNumber,
                ThreadLocalRandom.current().nextLong(1000),
                1,
                1,
                Optional.of(sortRange),
                Optional.empty());
    }

    private static ChunkIndexInfo shardWithTemporalRange(int bucketNumber, ChunkRange sortRange, ChunkRange temporalRange)
    {
        return new ChunkIndexInfo(
                1,
                bucketNumber,
                ThreadLocalRandom.current().nextLong(1000),
                1,
                1,
                Optional.of(sortRange),
                Optional.of(temporalRange));
    }

    private ChunkOrganizationManager createChunkOrganizationManager(long intervalMillis)
    {
        return new ChunkOrganizationManager(database,
                nodeIdCache,
                "node1",
                chunkManager,
                createChunkOrganizer(),
                TEMPORAL_FUNCTION,
                true,
                new Duration(intervalMillis, MILLISECONDS),
                new Duration(5, MINUTES));
    }
}
