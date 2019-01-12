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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.storage.organization.ShardOrganizationManager.createOrganizationSets;
import static com.facebook.presto.raptor.storage.organization.TestCompactionSetCreator.extractIndexes;
import static com.facebook.presto.raptor.storage.organization.TestShardOrganizer.createShardOrganizer;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardOrganizationManager
{
    private IDBI dbi;
    private Handle dummyHandle;
    private MetadataDao metadataDao;
    private ShardOrganizerDao organizerDao;

    private static final Table tableInfo = new Table(1L, OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), OptionalLong.empty(), true);
    private static final Table temporalTableInfo = new Table(1L, OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), OptionalLong.of(1), true);

    private static final List<Type> types = ImmutableList.of(BIGINT, VARCHAR, DATE, TIMESTAMP);
    private static final TemporalFunction TEMPORAL_FUNCTION = new TemporalFunction(UTC);

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        metadataDao = dbi.onDemand(MetadataDao.class);
        organizerDao = dbi.onDemand(ShardOrganizerDao.class);

        createTablesWithRetry(dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        dummyHandle.close();
    }

    @Test
    public void testOrganizationEligibleTables()
    {
        long table1 = metadataDao.insertTable("schema", "table1", false, true, null, 0);
        metadataDao.insertColumn(table1, 1, "foo", 1, "bigint", 1, null);

        metadataDao.insertTable("schema", "table2", false, true, null, 0);
        metadataDao.insertTable("schema", "table3", false, false, null, 0);
        assertEquals(metadataDao.getOrganizationEligibleTables(), ImmutableSet.of(table1));
    }

    @Test
    public void testTableDiscovery()
            throws Exception
    {
        long table1 = metadataDao.insertTable("schema", "table1", false, true, null, 0);
        metadataDao.insertColumn(table1, 1, "foo", 1, "bigint", 1, null);

        long table2 = metadataDao.insertTable("schema", "table2", false, true, null, 0);
        metadataDao.insertColumn(table2, 1, "foo", 1, "bigint", 1, null);

        metadataDao.insertTable("schema", "table3", false, false, null, 0);

        long intervalMillis = 100;
        ShardOrganizationManager organizationManager = createShardOrganizationManager(intervalMillis);

        // initializes tables
        Set<Long> actual = organizationManager.discoverAndInitializeTablesToOrganize();
        assertEquals(actual, ImmutableSet.of(table1, table2));

        // update the start times and test that the tables are discovered after interval seconds
        long updateTime = System.currentTimeMillis();
        organizerDao.updateLastStartTime("node1", table1, updateTime);
        organizerDao.updateLastStartTime("node1", table2, updateTime);

        // wait for some time (interval time) for the tables to be eligible for organization
        long start = System.nanoTime();
        while (organizationManager.discoverAndInitializeTablesToOrganize().isEmpty() &&
                nanosSince(start).toMillis() < intervalMillis + 1000) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(organizationManager.discoverAndInitializeTablesToOrganize(), ImmutableSet.of(table1, table2));
    }

    @Test
    public void testSimple()
    {
        long timestamp = 1L;
        int day = 1;

        List<ShardIndexInfo> shards = ImmutableList.of(
                shardWithSortRange(1, ShardRange.of(new Tuple(types, 5L, "hello", day, timestamp), new Tuple(types, 10L, "hello", day, timestamp))),
                shardWithSortRange(1, ShardRange.of(new Tuple(types, 7L, "hello", day, timestamp), new Tuple(types, 10L, "hello", day, timestamp))),
                shardWithSortRange(1, ShardRange.of(new Tuple(types, 6L, "hello", day, timestamp), new Tuple(types, 9L, "hello", day, timestamp))),
                shardWithSortRange(1, ShardRange.of(new Tuple(types, 1L, "hello", day, timestamp), new Tuple(types, 5L, "hello", day, timestamp))));
        Set<OrganizationSet> actual = createOrganizationSets(TEMPORAL_FUNCTION, tableInfo, shards);

        assertEquals(actual.size(), 1);
        // Shards 0, 1 and 2 are overlapping, so we should get an organization set with these shards
        assertEquals(getOnlyElement(actual).getShards(), extractIndexes(shards, 0, 1, 2));
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

        List<ShardIndexInfo> shards = ImmutableList.of(
                shardWithTemporalRange(1, ShardRange.of(new Tuple(types, 5L), new Tuple(types, 10L)), ShardRange.of(new Tuple(temporalType, day1), new Tuple(temporalType, day2))),
                shardWithTemporalRange(1, ShardRange.of(new Tuple(types, 7L), new Tuple(types, 10L)), ShardRange.of(new Tuple(temporalType, day4), new Tuple(temporalType, day5))),
                shardWithTemporalRange(1, ShardRange.of(new Tuple(types, 6L), new Tuple(types, 9L)), ShardRange.of(new Tuple(temporalType, day1), new Tuple(temporalType, day2))),
                shardWithTemporalRange(1, ShardRange.of(new Tuple(types, 4L), new Tuple(types, 8L)), ShardRange.of(new Tuple(temporalType, day4), new Tuple(temporalType, day5))));

        Set<OrganizationSet> organizationSets = createOrganizationSets(TEMPORAL_FUNCTION, temporalTableInfo, shards);
        Set<Set<UUID>> actual = organizationSets.stream()
                .map(OrganizationSet::getShards)
                .collect(toSet());

        // expect 2 organization sets, of overlapping shards (0, 2) and (1, 3)
        assertEquals(organizationSets.size(), 2);
        assertEquals(actual, ImmutableSet.of(extractIndexes(shards, 0, 2), extractIndexes(shards, 1, 3)));
    }

    private static ShardIndexInfo shardWithSortRange(int bucketNumber, ShardRange sortRange)
    {
        return new ShardIndexInfo(
                1,
                OptionalInt.of(bucketNumber),
                UUID.randomUUID(),
                1,
                1,
                Optional.of(sortRange),
                Optional.empty());
    }

    private static ShardIndexInfo shardWithTemporalRange(int bucketNumber, ShardRange sortRange, ShardRange temporalRange)
    {
        return new ShardIndexInfo(
                1,
                OptionalInt.of(bucketNumber),
                UUID.randomUUID(),
                1,
                1,
                Optional.of(sortRange),
                Optional.of(temporalRange));
    }

    private ShardOrganizationManager createShardOrganizationManager(long intervalMillis)
    {
        return new ShardOrganizationManager(dbi,
                "node1",
                createShardManager(dbi),
                createShardOrganizer(),
                TEMPORAL_FUNCTION,
                true,
                new Duration(intervalMillis, MILLISECONDS),
                new Duration(5, MINUTES));
    }
}
