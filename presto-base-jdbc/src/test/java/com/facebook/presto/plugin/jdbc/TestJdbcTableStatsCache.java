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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
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

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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
        // TTL=0 means expireAfterWrite(0): entries expire immediately, every call is a DB round-trip.
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(0), OptionalLong.empty());

        JdbcTableHandle handle = makeHandle("schema", "table");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "Both calls must reach the client when TTL=0 (disabled)");
        // TTL=0 still creates a Guava cache, so hit/miss counters are populated
        assertEquals(cacheStats.getTableStatisticsCacheHit(), 0L);
        assertEquals(cacheStats.getTableStatisticsCacheMiss(), 2L);
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
        TableStatistics result = cache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all());
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
        TableStatistics result = cache.getTableStatistics(SESSION, makeHandle("s", "customers"), ImmutableList.of(varcharCol), TupleDomain.all());

        assertTrue(result.getColumnStatistics().get(varcharCol).getRange().isPresent(), "Range stats must be passed through for any column type without modification");
    }

    @Test
    public void testCacheKeyEquality()
    {
        JdbcTableHandle handle1 = makeHandle("schema", "orders");
        JdbcTableHandle handle2 = makeHandle("schema", "orders");
        JdbcTableHandle handle3 = makeHandle("schema", "customers");

        assertEquals(makeHandle("schema", "orders"), makeHandle("schema", "orders"), "Equal handles must be equal (sanity check for cache key correctness)");
        assertNotEquals(handle1, handle3, "Different table handles must not be equal");

        // Verify that two equal handles produce identical cache behaviour
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());
        cache.getTableStatistics(SESSION, handle1, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle2, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Two handles for the same table must share one cache entry");
    }

    @Test
    public void testTtlZeroMeansCallThroughEveryTime()
    {
        // OptionalLong.of(0) -> expireAfterWrite(0) -> immediate eviction -> every call is a miss.
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(0), OptionalLong.empty());

        JdbcTableHandle handle = makeHandle("schema", "table");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "TTL=0 must call through to DB every time (immediate eviction)");
    }

    @Test
    public void testTransactionCacheDeduplicatesWithinQueryWhenGlobalDisabled()
    {
        // Global stats cache has TTL=0 (disabled - every call is a miss).
        // Transaction cache has no TTL (entries live for the transaction lifetime).
        // The first call must hit the DB, subsequent calls within the same transaction must be served
        // from the transaction cache without any further DB round-trips.
        JdbcMetadataCache globalCache = newCache(TableStatistics.empty(), OptionalLong.of(0), OptionalLong.empty());
        JdbcMetadataCache transactionCache = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);

        JdbcTableHandle handle = makeHandle("schema", "orders");

        transactionCache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        transactionCache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        transactionCache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Transaction cache must deduplicate repeated stats calls within the same query even when global cache is disabled");
    }

    @Test
    public void testTransactionCacheUsesGlobalCacheWhenEnabled()
    {
        // Global stats cache has TTL=1 hour (enabled).
        // Two separate transaction caches represent two successive queries.
        // Query 1 must hit the DB once, query 2 must be served from the global cache - zero DB calls.
        JdbcMetadataCache globalCache = newCache(TableStatistics.empty(), OptionalLong.of(3_600_000), OptionalLong.empty());
        JdbcMetadataCacheStats tx1Stats = new JdbcMetadataCacheStats();
        JdbcMetadataCache tx1 = JdbcMetadataCache.createTransactionCache(globalCache, tx1Stats, 1000);
        JdbcMetadataCacheStats tx2Stats = new JdbcMetadataCacheStats();
        JdbcMetadataCache tx2 = JdbcMetadataCache.createTransactionCache(globalCache, tx2Stats, 1000);

        JdbcTableHandle handle = makeHandle("schema", "orders");

        // Query 1: first call populates global cache
        tx1.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1, "First query must trigger one DB call");

        // Query 2: global cache is warm, no DB call needed
        tx2.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1, "Second query must be served from global cache without any DB call");
    }

    @Test
    public void testNewTransactionDoesNotReusePreviousTransactionCacheEntries()
    {
        // With global TTL=0 (disabled), each new transaction must start cold.
        // Every transaction's first call for any table must reach the DB.
        JdbcMetadataCache globalCache = newCache(TableStatistics.empty(), OptionalLong.of(0), OptionalLong.empty());
        JdbcTableHandle handle = makeHandle("schema", "orders");

        JdbcMetadataCache tx1 = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);
        tx1.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1);

        // tx1 is discarded, tx2 is a brand-new transaction - must not inherit tx1's entries
        JdbcMetadataCache tx2 = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);
        tx2.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "New transaction must not reuse the previous transaction's cached stats (global cache is disabled)");
    }

    @Test
    public void testTransactionCacheDifferentTablesLoadSeparately()
    {
        // Within one transaction, different tables must each trigger their own DB call.
        JdbcMetadataCache globalCache = newCache(TableStatistics.empty(), OptionalLong.of(0), OptionalLong.empty());
        JdbcMetadataCache transactionCache = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);

        transactionCache.getTableStatistics(SESSION, makeHandle("schema", "orders"), ImmutableList.of(), TupleDomain.all());
        transactionCache.getTableStatistics(SESSION, makeHandle("schema", "lineitem"), ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 2, "Each distinct table must be loaded independently even inside a transaction cache");
    }

    @Test
    public void testTransactionCacheSameTableDifferentProjectionsShareOneFetch()
    {
        // Within one transaction, two calls for the same table with different column subsets
        // must share a single DB fetch. The transaction cache stores the full CachedStats snapshot
        // and buildSlice() projects the requested columns on each read.
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

        JdbcMetadataCache globalCache = newCache(fullStats, OptionalLong.of(0), OptionalLong.empty());
        JdbcMetadataCache transactionCache = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);
        JdbcTableHandle handle = makeHandle("schema", "orders");

        TableStatistics resultA = transactionCache.getTableStatistics(SESSION, handle, ImmutableList.of(colA), TupleDomain.all());
        TableStatistics resultB = transactionCache.getTableStatistics(SESSION, handle, ImmutableList.of(colB), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Transaction cache must serve different projections from one shared fetch");
        assertEquals(resultA.getRowCount(), Estimate.of(100));
        assertEquals(resultA.getColumnStatistics().get(colA).getDistinctValuesCount(), statsA.getDistinctValuesCount());
        assertEquals(resultB.getRowCount(), Estimate.of(100));
        assertEquals(resultB.getColumnStatistics().get(colB).getDistinctValuesCount(), statsB.getDistinctValuesCount());
    }

    @Test
    public void testPrestoExceptionPropagatedThroughGlobalCache()
    {
        // When the underlying jdbcClient throws a PrestoException, the cache layer must
        // unwrap it and rethrow the original PrestoException - not an UncheckedExecutionException.
        JdbcMetadataCache globalCache = newCacheWithThrowingClient(
                new PrestoException(GENERIC_INTERNAL_ERROR, "stats fetch failed"),
                OptionalLong.of(3_600_000),
                OptionalLong.empty());

        PrestoException thrown = expectThrows(PrestoException.class,
                () -> globalCache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all()));

        assertEquals(thrown.getMessage(), "stats fetch failed", "PrestoException from client must propagate unwrapped through the global cache");
    }

    @Test
    public void testPrestoExceptionPropagatedThroughTransactionCache()
    {
        // When the underlying jdbcClient throws a PrestoException, the transaction-cache layer
        // must also propagate it unwrapped - not wrapped in UncheckedExecutionException.
        JdbcMetadataCache globalCache = newCacheWithThrowingClient(
                new PrestoException(GENERIC_INTERNAL_ERROR, "stats fetch failed"),
                OptionalLong.of(0),
                OptionalLong.empty());
        JdbcMetadataCache transactionCache = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);

        PrestoException thrown = expectThrows(PrestoException.class,
                () -> transactionCache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all()));

        assertEquals(thrown.getMessage(), "stats fetch failed", "PrestoException from client must propagate unwrapped through the transaction cache");
    }

    @Test
    public void testNonPrestoRuntimeExceptionWrappedInUncheckedExecutionException()
    {
        // A non-PrestoException runtime exception from the client must be rethrown as-is
        RuntimeException cause = new RuntimeException("unexpected DB failure");
        JdbcMetadataCache globalCache = newCacheWithThrowingClient(cause, OptionalLong.of(3_600_000), OptionalLong.empty());

        UncheckedExecutionException thrown = expectThrows(UncheckedExecutionException.class,
                () -> globalCache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all()));

        assertSame(thrown.getCause(), cause, "Non-PrestoException must be rethrown as UncheckedExecutionException with original cause");
    }

    @Test
    public void testBuildSliceWithNoColumnsReturnsRowCountOnly()
    {
        // When getTableStatistics is called with an empty column list, the result must carry the
        // row count but have zero column-statistics entries.
        JdbcColumnHandle colA = makeColumnHandle("col_a", Types.BIGINT, BigintType.BIGINT);
        ColumnStatistics statsA = ColumnStatistics.builder()
                .setDistinctValuesCount(Estimate.of(5))
                .setNullsFraction(Estimate.of(0.0))
                .build();
        TableStatistics fullStats = TableStatistics.builder()
                .setRowCount(Estimate.of(42))
                .setColumnStatistics(colA, statsA)
                .build();

        JdbcMetadataCache cache = newCache(fullStats, OptionalLong.of(60_000), OptionalLong.empty());
        TableStatistics result = cache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all());

        assertEquals(result.getRowCount(), Estimate.of(42), "Row count must be populated even with empty column list");
        assertTrue(result.getColumnStatistics().isEmpty(), "Column stats must be empty when no columns are requested");
    }

    @Test
    public void testBuildSliceWhenCachedStatsIsEmptyReturnsEmpty()
    {
        // CachedStats with unknown rowCount and empty column stats -> buildSlice must return
        // TableStatistics.empty().  This is the path taken when the connector returns empty().
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());
        JdbcColumnHandle colA = makeColumnHandle("col_a", Types.BIGINT, BigintType.BIGINT);

        TableStatistics result = cache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(colA), TupleDomain.all());

        assertSame(result, TableStatistics.empty(), "Unknown rowCount + no column stats in cache must produce TableStatistics.empty()");
    }

    @Test
    public void testProjectionSliceReturnsOnlyRequestedColumns()
    {
        // The client returns stats for A, B, C.  Requesting only B and C must produce a result that contains B and C but NOT A.
        JdbcColumnHandle colA = makeColumnHandle("col_a", Types.BIGINT, BigintType.BIGINT);
        JdbcColumnHandle colB = makeColumnHandle("col_b", Types.BIGINT, BigintType.BIGINT);
        JdbcColumnHandle colC = makeColumnHandle("col_c", Types.VARCHAR, VarcharType.createVarcharType(32));

        TableStatistics fullStats = TableStatistics.builder()
                .setRowCount(Estimate.of(200))
                .setColumnStatistics(colA, ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(10)).setNullsFraction(Estimate.of(0.0)).build())
                .setColumnStatistics(colB, ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(20)).setNullsFraction(Estimate.of(0.1)).build())
                .setColumnStatistics(colC, ColumnStatistics.builder().setDistinctValuesCount(Estimate.of(3)).setNullsFraction(Estimate.of(0.5)).build())
                .build();

        JdbcMetadataCache cache = newCache(fullStats, OptionalLong.of(60_000), OptionalLong.empty());
        TableStatistics result = cache.getTableStatistics(SESSION, makeHandle("s", "multi"), ImmutableList.of(colB, colC), TupleDomain.all());

        assertEquals(result.getRowCount(), Estimate.of(200));
        assertEquals(result.getColumnStatistics().size(), 2, "Only requested columns must appear in the result");
        assertNull(result.getColumnStatistics().get(colA), "colA must NOT be present in the projected result");
        assertTrue(result.getColumnStatistics().containsKey(colB), "colB must be present");
        assertTrue(result.getColumnStatistics().containsKey(colC), "colC must be present");
    }

    @Test
    public void testCacheStatsReturnZeroBeforeTableStatisticsCacheIsSet()
    {
        // A freshly constructed JdbcMetadataCacheStats (tableStatisticsCache not yet set)
        // must return 0 for all stats accessors without throwing NullPointerException.
        JdbcMetadataCacheStats stats = new JdbcMetadataCacheStats();

        assertEquals(stats.getTableStatisticsCacheHit(), 0L, "hit must be 0 before cache is set");
        assertEquals(stats.getTableStatisticsCacheMiss(), 0L, "miss must be 0 before cache is set");
        assertEquals(stats.getTableStatisticsCacheEviction(), 0L, "eviction must be 0 before cache is set");
        assertEquals(stats.getTableStatisticsCacheSize(), 0L, "size must be 0 before cache is set");
        assertEquals(stats.getTableStatisticsCacheLoadSuccessCount(), 0L, "loadSuccess must be 0 before cache is set");
        assertEquals(stats.getTableStatisticsCacheLoadExceptionCount(), 0L, "loadException must be 0 before cache is set");
        assertEquals(stats.getTableStatisticsCacheAverageLoadPenalty(), 0.0, "avgLoadPenalty must be 0.0 before cache is set");
    }

    @Test
    public void testCacheStatsSizeReflectsPopulatedEntries()
    {
        // After loading two distinct tables the size counter must equal 2.
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(60_000), OptionalLong.empty());
        cache.getTableStatistics(SESSION, makeHandle("s", "t1"), ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, makeHandle("s", "t2"), ImmutableList.of(), TupleDomain.all());

        assertEquals(cacheStats.getTableStatisticsCacheSize(), 2L, "Cache size must equal the number of distinct loaded tables");
    }

    @Test
    public void testRefreshIntervalIgnoredWhenEqualToTtl()
    {
        // JdbcMetadataCache only sets refreshAfterWrite when refreshInterval < TTL.
        // When they are equal the refresh interval must be ignored, so the second call must still be a cache hit (not a reload).
        JdbcMetadataCache cache = new JdbcMetadataCache(
                Executors.newSingleThreadExecutor(),
                makeClient(TableStatistics.empty()),
                cacheStats,
                OptionalLong.of(0),
                OptionalLong.empty(),
                10000,
                OptionalLong.of(5_000),
                OptionalLong.of(5_000),
                10000);

        JdbcTableHandle handle = makeHandle("s", "t");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "refreshInterval == TTL must be ignored; second call must be served from cache");
    }

    @Test
    public void testNoExpiryWhenStatsTtlIsEmpty()
    {
        // OptionalLong.empty() for statisticsCacheTtl means no expireAfterWrite is configured,
        // so entries live until evicted by size.  A second call must be a cache hit.
        JdbcMetadataCache cache = new JdbcMetadataCache(
                Executors.newSingleThreadExecutor(),
                makeClient(TableStatistics.empty()),
                cacheStats,
                OptionalLong.of(0),
                OptionalLong.empty(),
                10000,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10000);

        JdbcTableHandle handle = makeHandle("s", "no_expiry");
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());

        assertEquals(loadCount.get(), 1, "Empty stats TTL must mean no expiry; second call must hit the cache");
        assertEquals(cacheStats.getTableStatisticsCacheHit(), 1L);
        assertEquals(cacheStats.getTableStatisticsCacheMiss(), 1L);
    }

    @Test
    public void testLoadExceptionCountIncrementsOnClientFailure()
    {
        // When the client throws, Guava records it as a load exception.
        // loadExceptionCount must be >= 1 after the throwing call.
        JdbcMetadataCache cache = newCacheWithThrowingClient(
                new PrestoException(GENERIC_INTERNAL_ERROR, "boom"),
                OptionalLong.of(60_000),
                OptionalLong.empty());

        expectThrows(PrestoException.class,
                () -> cache.getTableStatistics(SESSION, makeHandle("s", "t"), ImmutableList.of(), TupleDomain.all()));

        assertEquals(cacheStats.getTableStatisticsCacheLoadExceptionCount(), 1L, "loadExceptionCount must be 1 after one client-thrown exception");
        assertEquals(cacheStats.getTableStatisticsCacheLoadSuccessCount(), 0L, "loadSuccessCount must remain 0 when every load throws");
    }

    @Test
    public void testInvalidateTableClearsStatsCacheEntry()
    {
        // After invalidateTable() the next stats call must trigger a new DB load,
        // not be served from the stale cached entry.
        JdbcMetadataCache cache = newCache(TableStatistics.empty(), OptionalLong.of(3_600_000), OptionalLong.empty());
        JdbcTableHandle handle = makeHandle("schema", "orders");

        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1);

        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1, "Second call must be a cache hit before invalidation");

        // Simulate a DDL operation (e.g. ADD COLUMN) that invalidates the cache entry
        cache.invalidateTable(SESSION, handle);

        // Entry was evicted -> must trigger a fresh DB load
        cache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 2, "invalidateTable() must evict the stats cache entry so the next call reloads from DB");
    }

    @Test
    public void testInvalidateTableOnTransactionCachePropagatesToGlobalCache()
    {
        // invalidateTable() called on a transaction cache must propagate to the global cache
        // via the delegate chain so that the next query (new transaction) also gets fresh stats.
        JdbcMetadataCache globalCache = newCache(TableStatistics.empty(), OptionalLong.of(3_600_000), OptionalLong.empty());
        JdbcMetadataCache txCache = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);
        JdbcTableHandle handle = makeHandle("schema", "orders");

        txCache.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 1, "First call must trigger one DB load");

        txCache.invalidateTable(SESSION, handle);

        JdbcMetadataCache tx2 = JdbcMetadataCache.createTransactionCache(globalCache, new JdbcMetadataCacheStats(), 1000);
        tx2.getTableStatistics(SESSION, handle, ImmutableList.of(), TupleDomain.all());
        assertEquals(loadCount.get(), 2, "After invalidation propagated to global cache, the next transaction must reload from DB");
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
                OptionalLong.of(3_600_000),
                OptionalLong.empty(),
                1);

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
        assertEquals(loadCount.get(), 3, "table1 must be re-fetched from DB after eviction (maximumSize=1 must have displaced it)");

        assertTrue(cacheStats.getTableStatisticsCacheEviction() >= 1, "evictionCount must be >= 1 after a size-based eviction");
    }

    private JdbcMetadataCache newCache(TableStatistics returnValue, OptionalLong statisticsTtl, OptionalLong statisticsRefresh)
    {
        return new JdbcMetadataCache(
                Executors.newSingleThreadExecutor(),
                makeClient(returnValue),
                cacheStats,
                OptionalLong.of(0),
                OptionalLong.empty(),
                10000,
                statisticsTtl,
                statisticsRefresh,
                10000);
    }

    private JdbcMetadataCache newCacheWithThrowingClient(RuntimeException toThrow, OptionalLong statisticsTtl, OptionalLong statisticsRefresh)
    {
        return new JdbcMetadataCache(
                Executors.newSingleThreadExecutor(),
                makeThrowingClient(toThrow),
                cacheStats,
                OptionalLong.of(0),
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

    private JdbcClient makeThrowingClient(RuntimeException toThrow)
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
                throw toThrow;
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
