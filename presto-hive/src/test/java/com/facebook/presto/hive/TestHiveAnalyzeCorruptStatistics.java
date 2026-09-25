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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.hive.HiveSessionProperties.COLLECT_COLUMN_STATISTICS_ON_WRITE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveAnalyzeCorruptStatistics
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "hive";
    private static final String SCHEMA = "test_analyze_corrupt_statistics_schema";
    private static final int COLUMN_COUNT = 1000;
    private static final int THREADS = 10;
    private static final int COLUMN_NAME_FIELD = 0;
    private static final int NULLS_FRACTION_FIELD = 3;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        queryRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .build();

        queryRunner.createCatalog(CATALOG, CATALOG, properties);
        queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));

        return queryRunner;
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @Test(invocationCount = 3)
    public void testAnalyzeCorruptColumnStatisticsOnEmptyTable()
            throws Exception
    {
        String tableName = "test_analyze_corrupt_column_statistics_" + randomTableSuffix();

        try {
            // Concurrent ANALYZE statements on a table whose column statistics are still empty are what leaves duplicated rows behind in the metastore column statistics.
            prepareTableWithoutColumnStatistics(tableName);
            assertEquals(columnsWithStatistics(tableName), 0, "expected the table to start without column statistics");

            analyzeConcurrently(tableName);
            assertEquals(columnsWithStatistics(tableName), COLUMN_COUNT, "expected every column to end up with statistics");

            // Reading and rewriting the statistics has to keep working afterwards.
            assertQuerySucceeds("SHOW STATS FOR " + tableName);
            assertQuerySucceeds("ANALYZE " + tableName);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void prepareTableWithoutColumnStatistics(String tableName)
    {
        List<String> columnNames = IntStream.rangeClosed(1, COLUMN_COUNT)
                .mapToObj(column -> "col_" + column + " integer")
                .collect(toImmutableList());
        List<String> columnValues = IntStream.rangeClosed(1, COLUMN_COUNT)
                .mapToObj(String::valueOf)
                .collect(toImmutableList());

        // Writing without column statistics is what CALL system.drop_stats(...) is used for on engines that have the procedure: it leaves the table with data but with no column statistics at all.
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, COLLECT_COLUMN_STATISTICS_ON_WRITE, "false")
                .build();

        assertUpdate(session, "CREATE TABLE " + tableName + " (" + join(",", columnNames) + ")");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (" + join(",", columnValues) + ")", 1);
    }

    private int columnsWithStatistics(String tableName)
    {
        MaterializedResult statistics = getQueryRunner().execute(getSession(), "SHOW STATS FOR " + tableName);
        return (int) statistics.getMaterializedRows().stream()
                .filter(row -> row.getField(COLUMN_NAME_FIELD) != null && row.getField(NULLS_FRACTION_FIELD) != null)
                .count();
    }

    private void analyzeConcurrently(String tableName)
            throws Exception
    {
        CyclicBarrier barrier = new CyclicBarrier(THREADS);
        ExecutorService executor = newFixedThreadPool(THREADS);
        try {
            List<Future<Optional<String>>> futures = IntStream.range(0, THREADS)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute(getSession(), "ANALYZE " + tableName);
                            return Optional.<String>empty();
                        }
                        catch (Exception e) {
                            return Optional.of(e.getMessage());
                        }
                    }))
                    .collect(toImmutableList());

            List<String> failures = futures.stream()
                    .map(future -> getFutureValue(future))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());

            assertTrue(failures.isEmpty(), format("%s of %s concurrent ANALYZE invocations failed: %s", failures.size(), THREADS, failures));
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
