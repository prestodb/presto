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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.operator.HashBuilderOperator;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLLECTION_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_CONSTRAINT_COLUMNS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_DOMAIN_RANGE_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_EXPECTED_PARTITIONS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSHED_INTO_SCAN;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_BEFORE_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_PROCESSED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_TIMED_OUT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_WAIT_TIME_NANOS;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for dynamic partition pruning with Iceberg tables.
 * <p>
 * Expected file counts are queried from the actual data layout via the {@code $path}
 * hidden column, making assertions independent of worker count.
 */
@Test(singleThreaded = true)
public class TestDynamicPartitionPruning
        extends AbstractTestQueryFramework
{
    private static final String DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE = DYNAMIC_FILTER_COLLECTION_TIME_NANOS + "[%s]";
    private static final String DYNAMIC_FILTER_EXPECTED_PARTITIONS_TEMPLATE = DYNAMIC_FILTER_EXPECTED_PARTITIONS + "[%s]";
    private static final String DYNAMIC_FILTER_TIMED_OUT_TEMPLATE = DYNAMIC_FILTER_TIMED_OUT + "[%s]";
    private static final String DYNAMIC_FILTER_DOMAIN_RANGE_COUNT_TEMPLATE = DYNAMIC_FILTER_DOMAIN_RANGE_COUNT + "[%s]";
    private static final String DISTRIBUTED_DYNAMIC_FILTER_STRATEGY = "distributed_dynamic_filter_strategy";
    private static final String DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME = "distributed_dynamic_filter_max_wait_time";
    private static final String DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS = "distributed_dynamic_filter_extended_metrics";

    private long factOrdersTotalFiles;
    private long factOrdersWestFiles;
    private long dimCustomersFiles;
    private long factOrdersByYearTotalFiles;
    private long factOrdersByYear2024Files;
    private long dimSelectedDatesFiles;
    private long factOrdersTotalManifests;
    private long factOrdersByYearTotalManifests;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .build()
                .getQueryRunner();

        // Install Hive plugin for CTE/exchange materialization temp tables.
        // Shares the same file-based metastore as the Iceberg catalog.
        Path catalogDirectory = getIcebergDataDirectoryPath(
                queryRunner.getCoordinator().getDataDirectory(), HIVE.name(), FileFormat.PARQUET, false);

        queryRunner.installPlugin(new HivePlugin("hive"));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.temporary-table-schema", "tpch")
                .put("hive.temporary-table-storage-format", "PAGEFILE")
                .build());

        return queryRunner;
    }

    @BeforeClass
    public void setupTestTables()
    {
        assertUpdate("CREATE TABLE fact_orders (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "amount DECIMAL(10, 2), " +
                "order_date DATE" +
                ") WITH (partitioning = ARRAY['customer_id'])");

        for (int customerId = 1; customerId <= 10; customerId++) {
            assertUpdate(format(
                    "INSERT INTO fact_orders " +
                            "SELECT " +
                            "  (row_number() OVER ()) + %d * 100 AS order_id, " +
                            "  CAST(%d AS BIGINT) AS customer_id, " +
                            "  CAST(random() * 1000 AS DECIMAL(10, 2)) AS amount, " +
                            "  DATE '2024-01-01' + INTERVAL '%d' DAY AS order_date " +
                            "FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 " +
                            "      UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t",
                    customerId, customerId, customerId),
                    10);
        }

        assertUpdate("CREATE TABLE dim_customers (" +
                "customer_id BIGINT, " +
                "customer_name VARCHAR, " +
                "region VARCHAR)");

        assertUpdate(
                "INSERT INTO dim_customers " +
                        "SELECT " +
                        "  n AS customer_id, " +
                        "  'Customer ' || CAST(n AS VARCHAR) AS customer_name, " +
                        "  CASE WHEN n <= 3 THEN 'WEST' ELSE 'EAST' END AS region " +
                        "FROM (SELECT row_number() OVER () AS n FROM " +
                        "     (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 " +
                        "      UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t) sq",
                10);

        assertUpdate("CREATE TABLE fact_orders_by_year (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "amount DECIMAL(10, 2), " +
                "order_date DATE" +
                ") WITH (partitioning = ARRAY['year(order_date)'])");

        assertUpdate(
                "INSERT INTO fact_orders_by_year VALUES " +
                        "(1, 1, DECIMAL '100.00', DATE '2022-03-15'), " +
                        "(2, 2, DECIMAL '200.00', DATE '2022-07-20'), " +
                        "(3, 3, DECIMAL '300.00', DATE '2022-11-01')",
                3);
        assertUpdate(
                "INSERT INTO fact_orders_by_year VALUES " +
                        "(4, 1, DECIMAL '150.00', DATE '2023-02-10'), " +
                        "(5, 2, DECIMAL '250.00', DATE '2023-06-15'), " +
                        "(6, 3, DECIMAL '350.00', DATE '2023-09-20')",
                3);
        assertUpdate(
                "INSERT INTO fact_orders_by_year VALUES " +
                        "(7, 1, DECIMAL '175.00', DATE '2024-01-05'), " +
                        "(8, 2, DECIMAL '275.00', DATE '2024-04-10'), " +
                        "(9, 3, DECIMAL '375.00', DATE '2024-08-25')",
                3);

        assertUpdate("CREATE TABLE dim_selected_dates (" +
                "order_date DATE, " +
                "label VARCHAR)");

        assertUpdate(
                "INSERT INTO dim_selected_dates VALUES " +
                        "(DATE '2024-01-05', 'JAN'), " +
                        "(DATE '2024-04-10', 'APR'), " +
                        "(DATE '2024-08-25', 'AUG')",
                3);

        assertUpdate("CREATE TABLE dim_active_regions (" +
                "region_name VARCHAR, " +
                "region_label VARCHAR)");

        assertUpdate(
                "INSERT INTO dim_active_regions VALUES ('WEST', 'West Coast')",
                1);

        factOrdersTotalFiles = countFiles("fact_orders");
        factOrdersWestFiles = countFiles("fact_orders", "customer_id IN (1, 2, 3)");
        dimCustomersFiles = countFiles("dim_customers");
        factOrdersByYearTotalFiles = countFiles("fact_orders_by_year");
        factOrdersByYear2024Files = countFiles("fact_orders_by_year",
                "order_date IN (DATE '2024-01-05', DATE '2024-04-10', DATE '2024-08-25')");
        dimSelectedDatesFiles = countFiles("dim_selected_dates");
        factOrdersTotalManifests = countManifests("fact_orders");
        factOrdersByYearTotalManifests = countManifests("fact_orders_by_year");
    }

    @AfterClass(alwaysRun = true)
    public void cleanupTestTables()
    {
        assertUpdate("DROP TABLE IF EXISTS fact_orders");
        assertUpdate("DROP TABLE IF EXISTS dim_customers");
        assertUpdate("DROP TABLE IF EXISTS fact_orders_by_year");
        assertUpdate("DROP TABLE IF EXISTS dim_selected_dates");
        assertUpdate("DROP TABLE IF EXISTS dim_active_regions");
    }

    @Test(invocationCount = 10)
    public void testDynamicPartitionPruningMetrics()
    {
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST'";

        ResultWithQueryId<MaterializedResult> result = executeWithDppSession(true, query);

        DynamicFilterInfo filterInfo = resolveDynamicFilter(result, "customer_id");
        assertNotNull(filterInfo, "Should resolve a dynamic filter from the plan");

        RuntimeStats runtimeStats = getRuntimeStats(result);

        assertEquals(getMetricValue(runtimeStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 1,
                "DF should be pushed into Iceberg scan exactly once");
        assertEquals(getMetricValue(runtimeStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersWestFiles,
                "Should process only WEST partition files");
        assertEquals(getMetricValue(runtimeStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "Filter resolved in time — no splits scheduled before filter");

        long waitTimeNanos = getMetricValue(runtimeStats, DYNAMIC_FILTER_WAIT_TIME_NANOS);
        assertTrue(waitTimeNanos >= 0, "Wait time should be non-negative");
        assertTrue(waitTimeNanos < SECONDS.toNanos(5),
                format("Dynamic filter should resolve before the 5s timeout, but waited %d ms",
                        NANOSECONDS.toMillis(waitTimeNanos)));

        long collectionTimeNanos = getMetricValue(runtimeStats, format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, filterInfo.filterId));
        assertTrue(collectionTimeNanos > 0,
                format("Per-filter collection time for filter %s should be positive", filterInfo.filterId));
        long buildSideNanos = getBuildSideWallNanos(result, filterInfo.joinNodeId);
        assertTrue(collectionTimeNanos <= gcTolerantUpperBound(buildSideNanos),
                format("Collection time (%d ms) exceeds build side bound (%d ms) for filter %s",
                        NANOSECONDS.toMillis(collectionTimeNanos), NANOSECONDS.toMillis(buildSideNanos), filterInfo.filterId));

        long skippedManifests = getIcebergScanMetric(runtimeStats, "fact_orders", "skippedDataManifests");
        long expectedSkippedManifests = factOrdersTotalManifests - 3;
        assertEquals(skippedManifests, expectedSkippedManifests,
                format("Iceberg ManifestEvaluator should skip %d non-WEST manifests (total=%d, WEST=3), but skipped %d",
                        expectedSkippedManifests, factOrdersTotalManifests, skippedManifests));
    }

    @Test(invocationCount = 10)
    public void testDynamicPartitionPruningResultCorrectness()
    {
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST' " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
        MaterializedResult dppResult = resultWithDpp.getResult();

        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);
        MaterializedResult noDppResult = resultWithoutDpp.getResult();

        assertEquals(dppResult.getRowCount(), 30,
                "DPP enabled should return all WEST rows");
        assertEquals(dppResult.getMaterializedRows(), noDppResult.getMaterializedRows(),
                "Results must be identical with DPP enabled vs disabled");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertEquals(getMetricValue(noDppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Without DPP, metric should not be emitted");

        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersWestFiles,
                "With DPP, only WEST partition files should be processed");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 1,
                "DF should be pushed into Iceberg scan");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        assertNotNull(filterInfo, "Should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "Result correctness");
        assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "Result correctness");

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders", "skippedDataManifests");
        assertEquals(dppSkippedManifests - noDppSkippedManifests, factOrdersTotalManifests - 3,
                format("DPP should skip exactly %d non-WEST manifests (total=%d, WEST=3)",
                        factOrdersTotalManifests - 3, factOrdersTotalManifests));

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "Result correctness");
    }

    @Test(invocationCount = 10)
    public void testBroadcastJoinDynamicPartitionPruning()
    {
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST' " +
                "ORDER BY f.order_id";

        Session dppSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "5s")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .setSystemProperty("join_distribution_type", "BROADCAST")
                .setCatalogSessionProperty("iceberg", "dynamic_filter_extended_metrics", "true")
                .build();

        ResultWithQueryId<MaterializedResult> resultWithDpp =
                getDistributedQueryRunner().executeWithQueryId(dppSession, query);
        MaterializedResult dppResult = resultWithDpp.getResult();

        Session noDppSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "DISABLED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "5s")
                .setSystemProperty("join_distribution_type", "BROADCAST")
                .build();

        ResultWithQueryId<MaterializedResult> resultWithoutDpp =
                getDistributedQueryRunner().executeWithQueryId(noDppSession, query);
        MaterializedResult noDppResult = resultWithoutDpp.getResult();

        assertEquals(dppResult.getRowCount(), 30,
                "Broadcast DPP should return all WEST rows");
        assertEquals(dppResult.getMaterializedRows(), noDppResult.getMaterializedRows(),
                "Results must be identical with DPP enabled vs disabled (broadcast join)");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 1,
                "Broadcast: DF should be pushed into Iceberg scan");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersWestFiles,
                "Broadcast: should process only WEST partition files with DPP");
        assertEquals(getMetricValue(noDppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Broadcast: metric should not be emitted without DPP");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        assertNotNull(filterInfo, "Broadcast: should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "Broadcast");
        assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "Broadcast");

        String rangeCountKey = format(DYNAMIC_FILTER_DOMAIN_RANGE_COUNT_TEMPLATE, filterInfo.filterId);
        assertTrue(getMetricValue(dppStats, rangeCountKey) > 0,
                format("Broadcast: domain range count for filter %s should be positive", filterInfo.filterId));

        long splitsWithoutFilter = getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER);
        long splitsProcessed = getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED);
        assertTrue(splitsWithoutFilter > splitsProcessed,
                format("Broadcast: splitsWithoutFilter (%d) should exceed splitsProcessed (%d)",
                        splitsWithoutFilter, splitsProcessed));

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders", "skippedDataManifests");
        assertEquals(dppSkippedManifests - noDppSkippedManifests, factOrdersTotalManifests - 3,
                format("Broadcast: DPP should skip exactly %d non-WEST manifests",
                        factOrdersTotalManifests - 3));

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "Broadcast");
    }

    @Test(invocationCount = 10)
    public void testDynamicPartitionPruningDisabled()
    {
        ResultWithQueryId<MaterializedResult> result = executeWithDppSession(false,
                "SELECT f.order_id, f.amount, c.customer_name " +
                        "FROM fact_orders f " +
                        "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                        "WHERE c.region = 'WEST'");

        RuntimeStats runtimeStats = getRuntimeStats(result);

        assertEquals(getMetricValue(runtimeStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Without DPP, metric should not be emitted");
        assertEquals(getMetricValue(runtimeStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 0,
                "Without DPP, no filter should be pushed into scan");

        assertEquals(getIcebergScanMetric(runtimeStats, "fact_orders", "skippedDataManifests"), 0,
                "Without DPP, no manifests should be skipped");
        assertEquals(getIcebergScanMetric(runtimeStats, "fact_orders", "skippedDataFiles"), 0,
                "Without DPP, no data files should be skipped");
    }

    @Test(invocationCount = 10)
    public void testDynamicPartitionPruningTimeout()
    {
        // 1ms timeout forces all-or-nothing safety path
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST' " +
                "ORDER BY f.order_id";

        Session timeoutSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "1ms")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .build();

        ResultWithQueryId<MaterializedResult> resultWithTimeout =
                getDistributedQueryRunner().executeWithQueryId(timeoutSession, query);
        MaterializedResult timeoutResult = resultWithTimeout.getResult();

        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);
        MaterializedResult noDppResult = resultWithoutDpp.getResult();

        assertEquals(timeoutResult.getRowCount(), 30,
                "Timeout DPP should return all WEST rows (no incorrect pruning)");
        assertEquals(timeoutResult.getMaterializedRows(), noDppResult.getMaterializedRows(),
                "Timeout results must be identical to no-DPP results (all-or-nothing safety)");

        RuntimeStats timeoutStats = getRuntimeStats(resultWithTimeout);

        assertEquals(getMetricValue(timeoutStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 0,
                "Timed-out filter should not be pushed into scan");

        assertEquals(getMetricValue(timeoutStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersTotalFiles,
                "Timed-out DPP should process all fact files (filter not applied)");

        long splitsBeforeFilter = getMetricValue(timeoutStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER);
        assertEquals(splitsBeforeFilter, factOrdersTotalFiles,
                "All fact_orders splits should be 'before filter' when filter timed out");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithTimeout, "customer_id");
        if (filterInfo != null) {
            String timeoutKey = format(DYNAMIC_FILTER_TIMED_OUT_TEMPLATE, filterInfo.filterId);
            assertEquals(getMetricValue(timeoutStats, timeoutKey), 1,
                    format("Timeout: dynamicFilterTimedOut[%s] should be 1", filterInfo.filterId));
        }
    }

    @Test(invocationCount = 10)
    public void testMultiJoinDynamicPartitionPruning()
    {
        // Subquery forces dim_customers x dim_active_regions to execute first,
        // so customer_id DF collects only WEST values, not all 10.
        String query = "SELECT f.order_id, f.amount, dc.customer_name " +
                "FROM fact_orders f " +
                "JOIN (" +
                "  SELECT c.customer_id, c.customer_name " +
                "  FROM dim_customers c " +
                "  JOIN dim_active_regions r ON c.region = r.region_name" +
                ") dc ON f.customer_id = dc.customer_id " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);
        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);

        assertEquals(resultWithDpp.getResult().getRowCount(),
                resultWithoutDpp.getResult().getRowCount(),
                format("DPP (%d rows) and no-DPP (%d rows) should return same count",
                        resultWithDpp.getResult().getRowCount(),
                        resultWithoutDpp.getResult().getRowCount()));
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "Results must be identical with DPP enabled vs disabled (multi-join)");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                "Multi-join: at least one DF should be pushed into scan");

        long dppSplitsProcessed = getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED);
        assertTrue(dppSplitsProcessed < factOrdersTotalFiles,
                format("Multi-join DPP should process fewer splits than total: %d (DPP) vs %d (total)",
                        dppSplitsProcessed, factOrdersTotalFiles));

        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "Multi-join: all filters should resolve in time");
    }

    @Test(invocationCount = 10)
    public void testDynamicPartitionPruningWithYearTransform()
    {
        String query = "SELECT f.order_id, f.amount, f.order_date " +
                "FROM fact_orders_by_year f " +
                "JOIN dim_selected_dates d ON f.order_date = d.order_date " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
        MaterializedResult dppResult = resultWithDpp.getResult();

        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);
        MaterializedResult noDppResult = resultWithoutDpp.getResult();

        assertEquals(dppResult.getRowCount(), 3,
                "Should return exactly 3 rows from year 2024");
        assertEquals(dppResult.getMaterializedRows(), noDppResult.getMaterializedRows(),
                "Results must be identical with DPP enabled vs disabled (year transform)");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertEquals(getMetricValue(noDppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Without DPP, metric should not be emitted");

        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersByYear2024Files,
                "With DPP, only year=2024 files should be processed");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 1,
                "Year transform: DF should be pushed into Iceberg scan");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "order_date");
        assertNotNull(filterInfo, "Year transform: should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "Year transform");
        assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "Year transform");

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders_by_year", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders_by_year", "skippedDataManifests");
        assertEquals(dppSkippedManifests - noDppSkippedManifests, factOrdersByYearTotalManifests - 1,
                format("Year transform: DPP should skip exactly %d non-2024 manifests (total=%d, 2024=1)",
                        factOrdersByYearTotalManifests - 1, factOrdersByYearTotalManifests));

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "Year transform");
    }

    private long countFiles(String tableName)
    {
        return countFiles(tableName, "1=1");
    }

    private long countFiles(String tableName, String predicate)
    {
        MaterializedResult result = computeActual(
                format("SELECT COUNT(DISTINCT \"$path\") FROM %s WHERE %s", tableName, predicate));
        return (long) result.getMaterializedRows().get(0).getField(0);
    }

    private long countManifests(String tableName)
    {
        MaterializedResult result = computeActual(
                format("SELECT COUNT(*) FROM \"%s$manifests\"", tableName));
        return (long) result.getMaterializedRows().get(0).getField(0);
    }

    // Background scans contribute equally to DPP-on and DPP-off, so deltas isolate the DPP effect
    private long getIcebergScanMetric(RuntimeStats runtimeStats, String tableName, String metricName)
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String key = catalog + "." + schema + "." + tableName + ".scan." + metricName;
        if (runtimeStats.getMetrics().containsKey(key)) {
            return runtimeStats.getMetrics().get(key).getSum();
        }
        return 0;
    }

    private ResultWithQueryId<MaterializedResult> executeWithDppSession(boolean enabled, String sql)
    {
        Session.SessionBuilder builder = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, enabled ? "ALWAYS" : "DISABLED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "5s");
        if (enabled) {
            builder.setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true");
            builder.setCatalogSessionProperty("iceberg", "dynamic_filter_extended_metrics", "true");
        }
        return getDistributedQueryRunner().executeWithQueryId(builder.build(), sql);
    }

    private QueryStats getQueryStats(ResultWithQueryId<MaterializedResult> result)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId())
                .getQueryStats();
    }

    private RuntimeStats getRuntimeStats(ResultWithQueryId<MaterializedResult> result)
    {
        return getQueryStats(result).getRuntimeStats();
    }

    private long getMetricValue(RuntimeStats runtimeStats, String metricName)
    {
        if (runtimeStats.getMetrics().containsKey(metricName)) {
            return runtimeStats.getMetrics().get(metricName).getSum();
        }
        return 0;
    }

    private static long gcTolerantUpperBound(long buildSideNanos)
    {
        return buildSideNanos * 10 + SECONDS.toNanos(2);
    }

    private void assertFilterResolvesWithinTimeout(RuntimeStats stats, String label)
    {
        long waitTimeNanos = getMetricValue(stats, DYNAMIC_FILTER_WAIT_TIME_NANOS);
        assertTrue(waitTimeNanos < SECONDS.toNanos(5),
                format("%s: filter should resolve before 5s timeout, but waited %d ms",
                        label, NANOSECONDS.toMillis(waitTimeNanos)));
    }

    private void assertCollectionTimeBoundedByBuildSide(
            RuntimeStats stats,
            ResultWithQueryId<MaterializedResult> result,
            DynamicFilterInfo filterInfo,
            String label)
    {
        long collectionTimeNanos = getMetricValue(stats, format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, filterInfo.filterId));
        assertTrue(collectionTimeNanos > 0,
                format("%s: per-filter collection time for filter %s should be positive", label, filterInfo.filterId));
        long buildSideNanos = getBuildSideWallNanos(result, filterInfo.joinNodeId);
        assertTrue(collectionTimeNanos <= gcTolerantUpperBound(buildSideNanos),
                format("%s: collection time (%d ms) exceeds build side bound (%d ms) for filter %s",
                        label, NANOSECONDS.toMillis(collectionTimeNanos), NANOSECONDS.toMillis(buildSideNanos), filterInfo.filterId));
    }

    private void assertDppReducesData(
            ResultWithQueryId<MaterializedResult> dppResult,
            ResultWithQueryId<MaterializedResult> noDppResult,
            String label)
    {
        QueryStats dppQueryStats = getQueryStats(dppResult);
        QueryStats noDppQueryStats = getQueryStats(noDppResult);

        assertTrue(dppQueryStats.getProcessedInputDataSize().toBytes() < noDppQueryStats.getProcessedInputDataSize().toBytes(),
                format("%s: DPP should process less input data: %s (DPP) vs %s (no DPP)",
                        label, dppQueryStats.getProcessedInputDataSize(), noDppQueryStats.getProcessedInputDataSize()));
        assertTrue(dppQueryStats.getProcessedInputPositions() < noDppQueryStats.getProcessedInputPositions(),
                format("%s: DPP should process fewer input rows: %d (DPP) vs %d (no DPP)",
                        label, dppQueryStats.getProcessedInputPositions(), noDppQueryStats.getProcessedInputPositions()));
        assertTrue(dppQueryStats.getCompletedSplits() < noDppQueryStats.getCompletedSplits(),
                format("%s: DPP should complete fewer splits: %d (DPP) vs %d (no DPP)",
                        label, dppQueryStats.getCompletedSplits(), noDppQueryStats.getCompletedSplits()));
    }

    private static class DynamicFilterInfo
    {
        private final String filterId;
        private final PlanNodeId joinNodeId;

        DynamicFilterInfo(String filterId, PlanNodeId joinNodeId)
        {
            this.filterId = filterId;
            this.joinNodeId = joinNodeId;
        }
    }

    private DynamicFilterInfo resolveDynamicFilter(ResultWithQueryId<MaterializedResult> result, String columnPrefix)
    {
        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId());

        return getAllStages(queryInfo.getOutputStage()).stream()
                .flatMap(stage -> stage.getPlan().stream())
                .flatMap(fragment -> searchFrom(fragment.getRoot())
                        .where(node -> node instanceof JoinNode)
                        .<JoinNode>findAll()
                        .stream())
                .flatMap(joinNode -> joinNode.getDynamicFilters().entrySet().stream()
                        .filter(entry -> entry.getValue().getName().startsWith(columnPrefix))
                        .map(entry -> new DynamicFilterInfo(entry.getKey(), joinNode.getId())))
                .findFirst()
                .orElse(null);
    }

    private DynamicFilterInfo resolveSemiJoinDynamicFilter(ResultWithQueryId<MaterializedResult> result, String columnPrefix)
    {
        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId());

        return getAllStages(queryInfo.getOutputStage()).stream()
                .flatMap(stage -> stage.getPlan().stream())
                .flatMap(fragment -> searchFrom(fragment.getRoot())
                        .where(node -> node instanceof SemiJoinNode)
                        .<SemiJoinNode>findAll()
                        .stream())
                .flatMap(semiJoinNode -> semiJoinNode.getDynamicFilters().entrySet().stream()
                        .filter(entry -> entry.getValue().getName().startsWith(columnPrefix))
                        .map(entry -> new DynamicFilterInfo(entry.getKey(), semiJoinNode.getId())))
                .findFirst()
                .orElse(null);
    }

    @Test(invocationCount = 10)
    public void testStarSchemaMultipleFilters()
    {
        assertUpdate("CREATE TABLE fact_star_orders (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "product_id BIGINT, " +
                "amount DECIMAL(10, 2)" +
                ") WITH (partitioning = ARRAY['customer_id', 'product_id'])");

        for (int customerId = 1; customerId <= 3; customerId++) {
            for (int productId = 1; productId <= 4; productId++) {
                assertUpdate(format(
                        "INSERT INTO fact_star_orders VALUES (%d, %d, %d, DECIMAL '100.00')",
                        customerId * 100 + productId, customerId, productId),
                        1);
            }
        }

        assertUpdate("CREATE TABLE dim_premium_customers (" +
                "customer_id BIGINT, " +
                "segment VARCHAR)");
        assertUpdate("INSERT INTO dim_premium_customers VALUES (1, 'PREMIUM'), (2, 'PREMIUM')", 2);

        assertUpdate("CREATE TABLE dim_electronics_products (" +
                "product_id BIGINT, " +
                "category VARCHAR)");
        assertUpdate("INSERT INTO dim_electronics_products VALUES (1, 'ELECTRONICS'), (2, 'ELECTRONICS')", 2);

        try {
            String query = "SELECT f.order_id, f.amount, c.segment, p.category " +
                    "FROM fact_star_orders f " +
                    "JOIN dim_premium_customers c ON f.customer_id = c.customer_id " +
                    "JOIN dim_electronics_products p ON f.product_id = p.product_id " +
                    "ORDER BY f.order_id";

            ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
            ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

            assertEquals(resultWithDpp.getResult().getRowCount(), 4,
                    "Star schema: should return 4 rows (2 customers × 2 products)");
            assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                    resultWithoutDpp.getResult().getMaterializedRows(),
                    "Star schema: results must be identical with DPP enabled vs disabled");

            DynamicFilterInfo customerFilter = resolveDynamicFilter(resultWithDpp, "customer_id");
            DynamicFilterInfo productFilter = resolveDynamicFilter(resultWithDpp, "product_id");
            assertNotNull(customerFilter, "Should resolve customer_id dynamic filter from plan");
            assertNotNull(productFilter, "Should resolve product_id dynamic filter from plan");

            RuntimeStats dppStats = getRuntimeStats(resultWithDpp);

            assertFilterResolvesWithinTimeout(dppStats, "Star schema");
            assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, customerFilter, "Star schema customer_id");
            assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, productFilter, "Star schema product_id");

            assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                    "Star schema: at least one DF should be pushed into scan");
            assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                    "Star schema: filters resolved in time — no splits scheduled before filter");

            assertDppReducesData(resultWithDpp, resultWithoutDpp, "Star schema");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS fact_star_orders");
            assertUpdate("DROP TABLE IF EXISTS dim_premium_customers");
            assertUpdate("DROP TABLE IF EXISTS dim_electronics_products");
        }
    }

    @Test(invocationCount = 10)
    public void testStarSchemaProgressiveRefinement()
    {
        assertUpdate("CREATE TABLE fact_progressive (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "product_id BIGINT, " +
                "amount DECIMAL(10, 2)" +
                ") WITH (partitioning = ARRAY['customer_id', 'product_id'])");

        for (int customerId = 1; customerId <= 3; customerId++) {
            for (int productId = 1; productId <= 4; productId++) {
                assertUpdate(format(
                        "INSERT INTO fact_progressive VALUES (%d, %d, %d, DECIMAL '100.00')",
                        customerId * 100 + productId, customerId, productId),
                        1);
            }
        }

        assertUpdate("CREATE TABLE dim_progressive_customers (" +
                "customer_id BIGINT, " +
                "segment VARCHAR)");
        assertUpdate("INSERT INTO dim_progressive_customers VALUES (1, 'PREMIUM'), (2, 'PREMIUM')", 2);

        assertUpdate("CREATE TABLE dim_progressive_products (" +
                "product_id BIGINT, " +
                "category VARCHAR)");
        assertUpdate("INSERT INTO dim_progressive_products VALUES (1, 'ELECTRONICS'), (2, 'ELECTRONICS')", 2);

        try {
            String query = "SELECT f.order_id, f.amount, c.segment, p.category " +
                    "FROM fact_progressive f " +
                    "JOIN dim_progressive_customers c ON f.customer_id = c.customer_id " +
                    "JOIN dim_progressive_products p ON f.product_id = p.product_id " +
                    "ORDER BY f.order_id";

            ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
            ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

            assertEquals(resultWithDpp.getResult().getRowCount(), 4,
                    "Progressive refinement: should return 4 rows (2 customers x 2 products)");
            assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                    resultWithoutDpp.getResult().getMaterializedRows(),
                    "Progressive refinement: results must be identical with DPP enabled vs disabled");

            DynamicFilterInfo customerFilter = resolveDynamicFilter(resultWithDpp, "customer_id");
            DynamicFilterInfo productFilter = resolveDynamicFilter(resultWithDpp, "product_id");
            assertNotNull(customerFilter, "Should resolve customer_id dynamic filter from plan");
            assertNotNull(productFilter, "Should resolve product_id dynamic filter from plan");

            RuntimeStats dppStats = getRuntimeStats(resultWithDpp);

            long customerCollectionTime = getMetricValue(dppStats,
                    format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, customerFilter.filterId));
            long productCollectionTime = getMetricValue(dppStats,
                    format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, productFilter.filterId));
            assertTrue(customerCollectionTime > 0,
                    format("Per-filter collection time for customer_id filter %s should be positive", customerFilter.filterId));
            assertTrue(productCollectionTime > 0,
                    format("Per-filter collection time for product_id filter %s should be positive", productFilter.filterId));

            assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_CONSTRAINT_COLUMNS), 2,
                    "Progressive refinement: constraint should cover 2 columns (customer_id + product_id)");

            assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                    "Progressive refinement: combined constraint should be pushed into scan");

            assertTrue(!customerFilter.joinNodeId.equals(productFilter.joinNodeId),
                    "Progressive refinement: filters should originate from different JoinNodes");

            assertFilterResolvesWithinTimeout(dppStats, "Progressive refinement");
            assertDppReducesData(resultWithDpp, resultWithoutDpp, "Progressive refinement");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS fact_progressive");
            assertUpdate("DROP TABLE IF EXISTS dim_progressive_customers");
            assertUpdate("DROP TABLE IF EXISTS dim_progressive_products");
        }
    }

    @Test(invocationCount = 10)
    public void testRightJoinDynamicPartitionPruning()
    {
        // Optimizer may convert to INNER, but validates AddDynamicFilterRule accepts RIGHT joins
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "RIGHT JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST' " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

        assertEquals(resultWithDpp.getResult().getRowCount(), 30,
                "RIGHT join DPP should return all matching rows");
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "RIGHT join: results must be identical with DPP enabled vs disabled");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                "RIGHT join: DF should be pushed into Iceberg scan");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersWestFiles,
                "RIGHT join: should process only WEST partition files with DPP");
        assertEquals(getMetricValue(noDppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "RIGHT join: metric should not be emitted without DPP");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "RIGHT join: filter resolved in time");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        assertNotNull(filterInfo, "RIGHT join: should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "RIGHT join");
        assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "RIGHT join");

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders", "skippedDataManifests");
        assertEquals(dppSkippedManifests - noDppSkippedManifests, factOrdersTotalManifests - 3,
                format("RIGHT join: DPP should skip exactly %d non-WEST manifests (total=%d, WEST=3)",
                        factOrdersTotalManifests - 3, factOrdersTotalManifests));

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "RIGHT join");
    }

    @Test(invocationCount = 10)
    public void testSemiJoinDynamicPartitionPruning()
    {
        // Optimizer may rewrite IN to JoinNode; fallback resolution handles both
        String query = "SELECT f.order_id, f.amount " +
                "FROM fact_orders f " +
                "WHERE f.customer_id IN (" +
                "  SELECT customer_id FROM dim_customers WHERE region = 'WEST'" +
                ") " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

        assertEquals(resultWithDpp.getResult().getRowCount(), 30,
                "Semi-join DPP should return all WEST rows");
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "Semi-join: results must be identical with DPP enabled vs disabled");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                "Semi-join: DF should be pushed into Iceberg scan");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersWestFiles,
                "Semi-join: should process only WEST partition files with DPP");
        assertEquals(getMetricValue(noDppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Semi-join: metric should not be emitted without DPP");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "Semi-join: filter resolved in time");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        if (filterInfo == null) {
            filterInfo = resolveSemiJoinDynamicFilter(resultWithDpp, "customer_id");
        }
        assertNotNull(filterInfo, "Semi-join: should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "Semi-join");

        long collectionTimeNanos = getMetricValue(dppStats, format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, filterInfo.filterId));
        assertTrue(collectionTimeNanos > 0,
                format("Semi-join: per-filter collection time for filter %s should be positive", filterInfo.filterId));

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders", "skippedDataManifests");
        assertEquals(dppSkippedManifests - noDppSkippedManifests, factOrdersTotalManifests - 3,
                format("Semi-join: DPP should skip exactly %d non-WEST manifests (total=%d, WEST=3)",
                        factOrdersTotalManifests - 3, factOrdersTotalManifests));

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "Semi-join");
    }

    @Test(invocationCount = 10)
    public void testNonSelectiveFilterNoPruning()
    {
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

        assertEquals(resultWithDpp.getResult().getRowCount(), 100,
                "Non-selective filter should return all 100 rows");
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "Non-selective: results must be identical with DPP enabled vs disabled");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                "Non-selective: DF should still be pushed into Iceberg scan");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersTotalFiles,
                "Non-selective: should process all fact files (no pruning benefit)");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "Non-selective: filter resolved in time");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        assertNotNull(filterInfo, "Non-selective: should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "Non-selective");
        assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "Non-selective");

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders", "skippedDataManifests");
        assertEquals(dppSkippedManifests, noDppSkippedManifests,
                "Non-selective: DPP should not skip any additional manifests");
    }

    @Test(invocationCount = 10)
    public void testEmptyBuildSidePrunesAll()
    {
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'NONEXISTENT' " +
                "ORDER BY f.order_id";

        ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
        ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

        assertEquals(resultWithDpp.getResult().getRowCount(), 0,
                "Empty build side should return 0 rows");
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "Empty build side: results must be identical with DPP enabled vs disabled");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
        RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

        assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                "Empty build side: DF should be pushed into Iceberg scan");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Empty build side: all fact splits pruned, zero processed");
        assertEquals(getMetricValue(noDppStats, DYNAMIC_FILTER_SPLITS_PROCESSED), 0,
                "Empty build side: metric should not be emitted without DPP");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "Empty build side: filter resolved in time");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        assertNotNull(filterInfo, "Empty build side: should resolve a dynamic filter from the plan");

        assertFilterResolvesWithinTimeout(dppStats, "Empty build side");

        // May be 0 for empty build side
        long collectionTimeNanos = getMetricValue(dppStats, format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, filterInfo.filterId));
        assertTrue(collectionTimeNanos >= 0,
                format("Empty build side: per-filter collection time for filter %s should be non-negative", filterInfo.filterId));
        long buildSideNanos = getBuildSideWallNanos(resultWithDpp, filterInfo.joinNodeId);
        assertTrue(collectionTimeNanos <= gcTolerantUpperBound(buildSideNanos),
                format("Empty build side: collection time (%d ms) exceeds build side bound (%d ms) for filter %s",
                        NANOSECONDS.toMillis(collectionTimeNanos), NANOSECONDS.toMillis(buildSideNanos), filterInfo.filterId));

        long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders", "skippedDataManifests");
        long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders", "skippedDataManifests");
        assertEquals(dppSkippedManifests - noDppSkippedManifests, factOrdersTotalManifests,
                format("Empty build side: DPP should skip all %d manifests", factOrdersTotalManifests));

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "Empty build side");
    }

    @Test(invocationCount = 10)
    public void testNonPartitionedProbeTable()
    {
        // Iceberg may still prune via file-level min/max, so assertions use soft bounds
        assertUpdate("CREATE TABLE fact_orders_unpartitioned (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "amount DECIMAL(10, 2))");

        assertUpdate(
                "INSERT INTO fact_orders_unpartitioned " +
                        "SELECT order_id, customer_id, amount FROM fact_orders",
                100);

        try {
            long unpartitionedTotalFiles = countFiles("fact_orders_unpartitioned");

            String query = "SELECT f.order_id, f.amount, c.customer_name " +
                    "FROM fact_orders_unpartitioned f " +
                    "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                    "WHERE c.region = 'WEST' " +
                    "ORDER BY f.order_id";

            ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
            ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

            assertEquals(resultWithDpp.getResult().getRowCount(), 30,
                    "Non-partitioned probe should return all WEST rows");
            assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                    resultWithoutDpp.getResult().getMaterializedRows(),
                    "Non-partitioned probe: results must be identical with DPP enabled vs disabled");

            RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
            RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

            assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                    "Non-partitioned probe: DF should be pushed into Iceberg scan");
            long dppSplitsProcessed = getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED);
            assertTrue(dppSplitsProcessed <= unpartitionedTotalFiles,
                    format("Non-partitioned probe: DPP should process no more than total files: %d (DPP) vs %d (total)",
                            dppSplitsProcessed, unpartitionedTotalFiles));
            assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                    "Non-partitioned probe: filter resolved in time");

            DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
            assertNotNull(filterInfo, "Non-partitioned probe: should resolve a dynamic filter from the plan");

            assertFilterResolvesWithinTimeout(dppStats, "Non-partitioned probe");
            assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "Non-partitioned probe");

            long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_orders_unpartitioned", "skippedDataManifests");
            long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_orders_unpartitioned", "skippedDataManifests");
            assertEquals(dppSkippedManifests, noDppSkippedManifests,
                    "Non-partitioned probe: no additional manifests should be skipped");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS fact_orders_unpartitioned");
        }
    }

    @Test(invocationCount = 10)
    public void testMultipleEquiJoinKeysDynamicPartitionPruning()
    {
        assertUpdate("CREATE TABLE fact_multi_key (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "region_code BIGINT, " +
                "amount DECIMAL(10, 2)" +
                ") WITH (partitioning = ARRAY['customer_id', 'region_code'])");

        for (int customerId = 1; customerId <= 3; customerId++) {
            for (int regionCode = 1; regionCode <= 2; regionCode++) {
                assertUpdate(format(
                        "INSERT INTO fact_multi_key VALUES (%d, %d, %d, DECIMAL '100.00')",
                        customerId * 10 + regionCode, customerId, regionCode),
                        1);
            }
        }

        assertUpdate("CREATE TABLE dim_multi_key (" +
                "customer_id BIGINT, " +
                "region_code BIGINT, " +
                "label VARCHAR)");
        assertUpdate("INSERT INTO dim_multi_key VALUES (1, 1, 'TARGET')", 1);

        try {
            long multiKeyTotalFiles = countFiles("fact_multi_key");
            long multiKeyTotalManifests = countManifests("fact_multi_key");
            long dimMultiKeyFiles = countFiles("dim_multi_key");

            String query = "SELECT f.order_id, f.amount, d.label " +
                    "FROM fact_multi_key f " +
                    "JOIN dim_multi_key d " +
                    "  ON f.customer_id = d.customer_id AND f.region_code = d.region_code " +
                    "ORDER BY f.order_id";

            ResultWithQueryId<MaterializedResult> resultWithDpp = executeWithDppSession(true, query);
            ResultWithQueryId<MaterializedResult> resultWithoutDpp = executeWithDppSession(false, query);

            assertEquals(resultWithDpp.getResult().getRowCount(), 1,
                    "Multi-key join should return 1 matching row");
            assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                    resultWithoutDpp.getResult().getMaterializedRows(),
                    "Multi-key: results must be identical with DPP enabled vs disabled");

            RuntimeStats dppStats = getRuntimeStats(resultWithDpp);
            RuntimeStats noDppStats = getRuntimeStats(resultWithoutDpp);

            assertTrue(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN) >= 1,
                    "Multi-key: DF should be pushed into Iceberg scan");
            long dppSplitsProcessed = getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED);
            assertTrue(dppSplitsProcessed < multiKeyTotalFiles,
                    format("Multi-key DPP should process fewer splits than total: %d (DPP) vs %d (total)",
                            dppSplitsProcessed, multiKeyTotalFiles));
            assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                    "Multi-key: filters resolved in time");

            DynamicFilterInfo customerFilter = resolveDynamicFilter(resultWithDpp, "customer_id");
            DynamicFilterInfo regionFilter = resolveDynamicFilter(resultWithDpp, "region_code");
            assertNotNull(customerFilter, "Should resolve customer_id dynamic filter");
            assertNotNull(regionFilter, "Should resolve region_code dynamic filter");

            assertEquals(customerFilter.joinNodeId, regionFilter.joinNodeId,
                    "Both filters should originate from the same JoinNode");

            assertFilterResolvesWithinTimeout(dppStats, "Multi-key");
            assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, customerFilter, "Multi-key customer_id");
            assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, regionFilter, "Multi-key region_code");

            long dppSkippedManifests = getIcebergScanMetric(dppStats, "fact_multi_key", "skippedDataManifests");
            long noDppSkippedManifests = getIcebergScanMetric(noDppStats, "fact_multi_key", "skippedDataManifests");
            assertTrue(dppSkippedManifests > noDppSkippedManifests,
                    format("Multi-key: DPP should skip more manifests: %d (DPP) vs %d (no DPP)",
                            dppSkippedManifests, noDppSkippedManifests));

            assertDppReducesData(resultWithDpp, resultWithoutDpp, "Multi-key");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS fact_multi_key");
            assertUpdate("DROP TABLE IF EXISTS dim_multi_key");
        }
    }

    @Test(invocationCount = 10)
    public void testHashWithLocalSplitsDynamicFilterCompletion()
    {
        // Regression test for SectionExecutionFactory: setExpectedPartitionsForFilters
        // must be called for HASH-partitioned stages that contain local table scans
        // (the "HASH-with-local-splits" code path at the `else if (!splitSources.isEmpty())`
        // branch). Without this, JoinDynamicFilter.expectedPartitions stays at
        // Integer.MAX_VALUE and the filter never completes, causing the split source
        // to wait until timeout.
        //
        // CTE self-join patterns with exchange materialization create fragments that
        // are HASH-partitioned with local bucketed temp table scans, triggering this
        // code path. We use a short max-wait-time (2s) so that if the fix is reverted,
        // the dynamic filter times out and collectionTimeNanos is not emitted.
        String query = "WITH order_agg AS (" +
                "  SELECT customer_id, SUM(amount) AS total_amount" +
                "  FROM fact_orders" +
                "  GROUP BY customer_id" +
                ") " +
                "SELECT a.customer_id, a.total_amount, b.total_amount AS total2 " +
                "FROM order_agg a " +
                "JOIN order_agg b ON a.customer_id = b.customer_id " +
                "ORDER BY a.customer_id";

        Session materializedSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "30s")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_EXTENDED_METRICS, "true")
                .setSystemProperty("cte_materialization_strategy", "ALL")
                .setSystemProperty("cte_partitioning_provider_catalog", "hive")
                .setSystemProperty("exchange_materialization_strategy", "ALL")
                .setSystemProperty("partitioning_provider_catalog", "hive")
                .setSystemProperty("grouped_execution", "true")
                .setSystemProperty("colocated_join", "true")
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        Session baselineSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "DISABLED")
                .setSystemProperty("cte_materialization_strategy", "ALL")
                .setSystemProperty("cte_partitioning_provider_catalog", "hive")
                .setSystemProperty("exchange_materialization_strategy", "ALL")
                .setSystemProperty("partitioning_provider_catalog", "hive")
                .setSystemProperty("grouped_execution", "true")
                .setSystemProperty("colocated_join", "true")
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        ResultWithQueryId<MaterializedResult> resultWithDpp =
                getDistributedQueryRunner().executeWithQueryId(materializedSession, query);
        ResultWithQueryId<MaterializedResult> resultWithoutDpp =
                getDistributedQueryRunner().executeWithQueryId(baselineSession, query);

        // Correctness: results must match regardless of DPP
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "HASH-with-local-splits: results must be identical with DPP enabled vs disabled");

        // Find dynamic filter from the CTE self-join
        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        if (filterInfo == null) {
            filterInfo = resolveDynamicFilter(resultWithDpp, "total_amount");
        }
        assertNotNull(filterInfo, "HASH-with-local-splits: should resolve a dynamic filter from the plan");

        // Verify setExpectedPartitionsForFilters was called for the HASH-with-local-splits path.
        // Without the fix, expectedPartitions stays at MAX_VALUE (metric never emitted).
        // We check expectedPartitions rather than collectionTimeNanos because the latter
        // is written by an async fetcher callback that may fire after the query's
        // finalQueryInfo snapshot is taken (the CTE temp table scan is not an Iceberg scan,
        // so the query doesn't block on the dynamic filter).
        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);

        long expectedPartitions = getMetricValue(dppStats,
                format(DYNAMIC_FILTER_EXPECTED_PARTITIONS_TEMPLATE, filterInfo.filterId));
        assertTrue(expectedPartitions > 0,
                format("Dynamic filter %s: expectedPartitions should be > 0 (setExpectedPartitionsForFilters " +
                        "must be called for HASH-with-local-splits path), but was %d",
                        filterInfo.filterId, expectedPartitions));

        // Verify all dynamic filters in the query had expectedPartitions set
        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(resultWithDpp.getQueryId());
        long totalFilters = getAllStages(queryInfo.getOutputStage()).stream()
                .flatMap(stage -> stage.getPlan().stream())
                .flatMap(fragment -> searchFrom(fragment.getRoot())
                        .where(node -> node instanceof JoinNode)
                        .<JoinNode>findAll()
                        .stream())
                .mapToLong(joinNode -> joinNode.getDynamicFilters().size())
                .sum();

        if (totalFilters > 0) {
            long configuredFilters = getAllStages(queryInfo.getOutputStage()).stream()
                    .flatMap(stage -> stage.getPlan().stream())
                    .flatMap(fragment -> searchFrom(fragment.getRoot())
                            .where(node -> node instanceof JoinNode)
                            .<JoinNode>findAll()
                            .stream())
                    .flatMap(joinNode -> joinNode.getDynamicFilters().keySet().stream())
                    .filter(filterId -> getMetricValue(dppStats,
                            format(DYNAMIC_FILTER_EXPECTED_PARTITIONS_TEMPLATE, filterId)) > 0)
                    .count();
            assertEquals(configuredFilters, totalFilters,
                    format("All %d dynamic filters should have expectedPartitions set, but only %d did",
                            totalFilters, configuredFilters));
        }
    }

    @Test(invocationCount = 10)
    public void testPartitionedJoinDynamicFilterCompletion()
    {
        // Regression test: when the JoinNode's build side is a RemoteSourceNode
        // (partitioned join), setExpectedPartitionsForFilters must still be called.
        // Without the fix, expectedPartitions stays at Integer.MAX_VALUE and
        // the filter never completes, causing the split source to wait until timeout.
        String query = "SELECT f.order_id, f.amount, c.customer_name " +
                "FROM fact_orders f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST' " +
                "ORDER BY f.order_id";

        Session dppSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "2s")
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        Session noDppSession = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "DISABLED")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "2s")
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        ResultWithQueryId<MaterializedResult> resultWithDpp =
                getDistributedQueryRunner().executeWithQueryId(dppSession, query);
        ResultWithQueryId<MaterializedResult> resultWithoutDpp =
                getDistributedQueryRunner().executeWithQueryId(noDppSession, query);

        // Correctness
        assertEquals(resultWithDpp.getResult().getRowCount(), 30,
                "Partitioned join DPP should return all WEST rows");
        assertEquals(resultWithDpp.getResult().getMaterializedRows(),
                resultWithoutDpp.getResult().getMaterializedRows(),
                "Partitioned join: results must be identical with DPP enabled vs disabled");

        DynamicFilterInfo filterInfo = resolveDynamicFilter(resultWithDpp, "customer_id");
        assertNotNull(filterInfo, "Partitioned join: should resolve a dynamic filter from the plan");

        RuntimeStats dppStats = getRuntimeStats(resultWithDpp);

        // Core assertion: expected partitions was set (> 0)
        long expectedPartitions = getMetricValue(dppStats,
                format(DYNAMIC_FILTER_EXPECTED_PARTITIONS_TEMPLATE, filterInfo.filterId));
        assertTrue(expectedPartitions > 0,
                format("Partitioned join: expected partitions for filter %s should be > 0, but was %d. " +
                        "setExpectedPartitionsForFilters may be skipping RemoteSourceNode build sides",
                        filterInfo.filterId, expectedPartitions));

        // Filter completed (collection time only emitted when fullyResolved = true)
        long collectionTimeNanos = getMetricValue(dppStats,
                format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, filterInfo.filterId));
        assertTrue(collectionTimeNanos > 0,
                format("Partitioned join: collection time for filter %s should be > 0 (filter should complete, not timeout)",
                        filterInfo.filterId));

        assertFilterResolvesWithinTimeout(dppStats, "Partitioned join");
        assertCollectionTimeBoundedByBuildSide(dppStats, resultWithDpp, filterInfo, "Partitioned join");

        // Split pruning
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_PUSHED_INTO_SCAN), 1,
                "Partitioned join: DF should be pushed into Iceberg scan");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_PROCESSED),
                factOrdersWestFiles,
                "Partitioned join: should process only WEST partition files with DPP");
        assertEquals(getMetricValue(dppStats, DYNAMIC_FILTER_SPLITS_BEFORE_FILTER), 0,
                "Partitioned join: filter resolved in time — no splits scheduled before filter");

        assertDppReducesData(resultWithDpp, resultWithoutDpp, "Partitioned join");
    }

    private long getBuildSideWallNanos(ResultWithQueryId<MaterializedResult> result, PlanNodeId joinNodeId)
    {
        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId());

        return getAllStages(queryInfo.getOutputStage()).stream()
                .flatMap(stage -> stage.getLatestAttemptExecutionInfo().getTasks().stream())
                .mapToLong(task -> task.getStats().getPipelines().stream()
                        .flatMap(pipeline -> pipeline.getOperatorSummaries().stream())
                        .filter(operator -> HashBuilderOperator.OPERATOR_TYPE.equals(operator.getOperatorType())
                                && operator.getPlanNodeId().equals(joinNodeId))
                        .mapToLong(operator -> operator.getAddInputWall().roundTo(NANOSECONDS)
                                + operator.getGetOutputWall().roundTo(NANOSECONDS)
                                + operator.getFinishWall().roundTo(NANOSECONDS))
                        .sum())
                .max()
                .orElse(0);
    }
}
