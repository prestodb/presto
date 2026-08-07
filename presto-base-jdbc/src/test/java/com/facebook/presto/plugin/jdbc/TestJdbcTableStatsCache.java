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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import org.h2.Driver;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcTableStatsCache
{
    private static final String CONNECTOR_ID = "test";

    private AtomicInteger loadCount;
    private JdbcMetadataCacheStats cacheStats;

    @BeforeMethod
    public void setUp()
    {
        loadCount = new AtomicInteger(0);
        cacheStats = new JdbcMetadataCacheStats();
    }

    @Test
    public void testCachingDisabledCallsThroughEveryTime()
    {
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.empty(), OptionalLong.empty());

        JdbcTableHandle handle = makeHandle("schema", "table");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "Both calls must reach the client when caching is disabled");
        // JMX stats report 0 when there is no cache
        assertEquals(cacheStats.getTableStatisticsCacheHit(), 0L);
        assertEquals(cacheStats.getTableStatisticsCacheMiss(), 0L);
    }

    @Test
    public void testCachingEnabledReturnsCachedResult()
    {
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());

        JdbcTableHandle handle = makeHandle("schema", "table");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Client must be called only once when caching is enabled");
        // Guava CacheStats: 1 miss (first call), 1 hit (second call)
        assertEquals(cacheStats.getTableStatisticsCacheHit(), 1L);
        assertEquals(cacheStats.getTableStatisticsCacheMiss(), 1L);
    }

    @Test
    public void testDifferentTablesLoadSeparately()
    {
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());

        cache.getTableStatistics(SESSION, makeHandle("schema", "table1"), ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, makeHandle("schema", "table2"), ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "Each distinct table must be loaded independently");
    }

    @Test
    public void testSameTableDifferentProjectionsShareOneFetch()
    {
        JdbcColumnHandle colA = makeColumnHandle("col_a", Types.BIGINT, BigintType.BIGINT);
        JdbcColumnHandle colB = makeColumnHandle("col_b", Types.VARCHAR, VarcharType.createVarcharType(50));

        ColumnStatistics statsA = ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(10))
                .setNullsFraction(Estimate.of(0.0))
                .build();
        ColumnStatistics statsB = ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(5))
                .setNullsFraction(Estimate.of(0.1))
                .build();

        TableStatistics fullStats = TableStatistics.builder()
                .setRowCount(Estimate.of(100))
                .setColumnStatistics(colA, statsA)
                .setColumnStatistics(colB, statsB)
                .build();

        JdbcMetadataCache cache = newCache(fullStats, OptionalLong.of(60_000), OptionalLong.empty());
        JdbcTableHandle handle = makeHandle("schema", "orders");

        TableStatistics resultA = cache.getTableStatistics(SESSION, handle, ImmutableList.of(colA), TupleDomain.all());
        TableStatistics resultB = cache.getTableStatistics(SESSION, handle, ImmutableList.of(colB), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Same table with different projections must share one DB fetch");

        assertEquals(resultA.getRowCount(), Estimate.of(100));
        assertEquals(resultA.getColumnStatistics().size(), 1);
        assertEquals(resultA.getColumnStatistics().get(colA).getDistinctValuesCount(), statsA.getDistinctValuesCount());

        assertEquals(resultB.getRowCount(), Estimate.of(100));
        assertEquals(resultB.getColumnStatistics().size(), 1);
        assertEquals(resultB.getColumnStatistics().get(colB).getDistinctValuesCount(), statsB.getDistinctValuesCount());
    }

    @Test
    public void testProjectedColumnNotInCacheReturnsEmptyStats()
    {
        // Client returns stats only for col_a, col_b is not in the DB result
        JdbcColumnHandle colA = makeColumnHandle("col_a", Types.BIGINT, BigintType.BIGINT);
        JdbcColumnHandle colB = makeColumnHandle("col_b", Types.BIGINT, BigintType.BIGINT);

        TableStatistics partialStats = TableStatistics.builder()
                .setRowCount(Estimate.of(50))
                .setColumnStatistics(colA, ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(3)).setNullsFraction(Estimate.of(0.0)).build())
                .build();

        JdbcMetadataCache cache = newCache(partialStats, OptionalLong.of(60_000), OptionalLong.empty());
        TableStatistics result = cache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(colB), TupleDomain.all());

        // col_b was not in the DB result, must come back as empty stats, not missing key
        assertTrue(result.getColumnStatistics().containsKey(colB), "Missing projected column must have a key in the result");
        assertSame(result.getColumnStatistics().get(colB), ColumnStatistics.empty(), "Missing projected column must return ColumnStatistics.empty()");
    }

    @Test
    public void testEmptyTableStatisticsFromClientReturnsEmpty()
    {
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());

        TableStatistics result = cache.getTableStatistics(
                SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all());

        assertSame(result, TableStatistics.empty(), "empty() from client must propagate as empty()");
    }

    @Test
    public void testRangeStatsPassedThroughForAllTypes()
    {
        JdbcColumnHandle varcharCol = makeColumnHandle("name", Types.VARCHAR, VarcharType.createVarcharType(50));
        ColumnStatistics statsWithRange = ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(7))
                .setNullsFraction(Estimate.of(0.0))
                .setRange(new DoubleRange(1.0, 99.0))
                .build();
        TableStatistics fullStats = TableStatistics.builder()
                .setRowCount(Estimate.of(50))
                .setColumnStatistics(varcharCol, statsWithRange)
                .build();

        JdbcMetadataCache cache = newCache(fullStats, OptionalLong.of(60_000), OptionalLong.empty());
        TableStatistics result = cache.getTableStatistics(
                SESSION, makeHandle("s", "customers"), ImmutableList.of(varcharCol), TupleDomain.all());

        assertTrue(result.getColumnStatistics().get(varcharCol).getRange().isPresent(),
                "Range stats must be passed through for any column type without modification");
    }

    @Test
    public void testCacheKeyEquality()
    {
        JdbcTableHandle handle1 = makeHandle("schema", "orders");
        JdbcTableHandle handle2 = makeHandle("schema", "orders");
        JdbcTableHandle handle3 = makeHandle("schema", "customers");

        assertEquals(makeHandle("schema", "orders"), makeHandle("schema", "orders"),
                "Equal handles must be equal (sanity check for cache key correctness)");
        assertNotEquals(handle1, handle3, "Different table handles must not be equal");

        // Verify that two equal handles produce identical cache behaviour
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());
        cache.getTableStatistics(SESSION, handle1, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle2, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Two handles for the same table must share one cache entry");
    }

    @Test
    public void testTtlZeroDisablesCaching()
    {
        // TTL passed as OptionalLong.of(0) must behave identically to OptionalLong.empty()
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(0), OptionalLong.empty());

        JdbcTableHandle handle = makeHandle("schema", "table");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "TTL of 0 must disable caching and call through every time");
    }

    @Test
    public void testMaximumSizeEvictsEntries()
    {
        JdbcMetadataCache cache = new JdbcMetadataCache(
                Executors.newSingleThreadExecutor(),
                makeClient(TableStatistics.empty()),
                cacheStats,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10000,
                OptionalLong.of(3_600_000), // 1 hour stats TTL
                OptionalLong.empty(),
                1); // statisticsCacheMaximumSize = 1

        JdbcTableHandle table1 = makeHandle("schema", "t1");
        JdbcTableHandle table2 = makeHandle("schema", "t2");

        // Load table1 -> miss #1, cache = {t1}
        cache.getTableStatistics(SESSION, table1, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1);

        // Load table2 -> miss #2, evicts t1, cache = {t2}
        cache.getTableStatistics(SESSION, table2, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 2);

        // Re-load table1 -> must be a miss (evicted), not a hit
        cache.getTableStatistics(SESSION, table1, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 3,
                "table1 must be re-fetched from DB after eviction (maximumSize=1 must have displaced it)");

        assertTrue(cacheStats.getTableStatisticsCacheEviction() >= 1,
                "evictionCount must be >= 1 after a size-based eviction");
    }

    private JdbcMetadataCache newCache(TableStatistics returnValue, OptionalLong statisticsTtl, OptionalLong statisticsRefresh)
    {
        return new JdbcMetadataCache(
                Executors.newSingleThreadExecutor(),
                makeClient(returnValue),
                cacheStats,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10000,
                statisticsTtl,
                statisticsRefresh,
                10000);
    }

    private JdbcClient makeClient(TableStatistics returnValue)
    {
        String connectionUrl = "jdbc:h2:mem:stats_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
        return new BaseJdbcClient(
                new JdbcConnectorId(CONNECTOR_ID),
                new BaseJdbcConfig().setConnectionUrl(connectionUrl),
                "\"",
                new DriverConnectionFactory(new Driver(), connectionUrl, Optional.empty(), Optional.empty(), new Properties()))
        {
            @Override
            public TableStatistics getTableStatistics(
                    ConnectorSession session,
                    JdbcTableHandle handle,
                    List<JdbcColumnHandle> columnHandles,
                    TupleDomain<ColumnHandle> tupleDomain)
            {
                loadCount.incrementAndGet();
                return returnValue;
            }
        };
    }

    private static JdbcTableHandle makeHandle(String schema, String table)
    {
        return new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), null, schema, table);
    }

    private static JdbcColumnHandle makeColumnHandle(String name, int jdbcType, Type type)
    {
        return new JdbcColumnHandle(
                CONNECTOR_ID,
                name,
                new JdbcTypeHandle(jdbcType, Integer.toString(jdbcType), 0, 0),
                type,
                true,
                Optional.empty());
    }
}
