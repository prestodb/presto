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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.iceberg.AbstractTestDynamicPartitionPruning;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_RUNTIME_STATS_ENABLED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNativeDynamicPartitionPruning
        extends AbstractTestDynamicPartitionPruning
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner)
                PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                        .setStorageFormat("PARQUET")
                        .setUseThrift(true)
                        .setAdditionalCatalogs(ImmutableMap.of("hive", "hive"))
                        .build();

        Path catalogDirectory = getIcebergDataDirectoryPath(
                queryRunner.getCoordinator().getDataDirectory(),
                HIVE.name(), FileFormat.PARQUET, false);

        queryRunner.installPlugin(new HivePlugin("hive"));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir",
                        catalogDirectory.toFile().toURI().toString())
                .put("hive.temporary-table-schema", "tpch")
                .put("hive.temporary-table-storage-format", "PARQUET")
                .put("hive.temporary-table-compression-codec", "NONE")
                .build());

        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .build();
    }

    @Override
    protected void executeTableDdl(String sql)
    {
        ((QueryRunner) getExpectedQueryRunner()).execute(sql);
    }

    @Override
    protected void executeTableDdl(String sql, long expectedRowCount)
    {
        ((QueryRunner) getExpectedQueryRunner()).execute(sql);
    }

    @BeforeClass
    public void setupLargeTable()
    {
        executeTableDdl("DROP TABLE IF EXISTS fact_orders_large");
        executeTableDdl("CREATE TABLE fact_orders_large (" +
                "order_id BIGINT, " +
                "customer_id BIGINT, " +
                "amount DECIMAL(10, 2), " +
                "order_date DATE)");
        executeTableDdl("INSERT INTO fact_orders_large " +
                "SELECT " +
                "  row_number() OVER () AS order_id, " +
                "  (row_number() OVER () - 1) % 10 + 1 AS customer_id, " +
                "  CAST(random() * 1000 AS DECIMAL(10, 2)) AS amount, " +
                "  DATE '2024-01-01' + INTERVAL '1' DAY * ((row_number() OVER () - 1) % 365) AS order_date " +
                "FROM (SELECT 1 FROM fact_orders CROSS JOIN " +
                "  (VALUES 1,2,3,4,5,6,7,8,9,10) v(x)) t");
    }

    @Test
    public void testDynamicFilterPushToWorker()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_MAX_WAIT_TIME, "5s")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(VERBOSE_RUNTIME_STATS_ENABLED, "true")
                .build();

        String query = "SELECT count(*) " +
                "FROM fact_orders_large f " +
                "JOIN dim_customers c ON f.customer_id = c.customer_id " +
                "WHERE c.region = 'WEST'";

        ResultWithQueryId<MaterializedResult> result =
                getDistributedQueryRunner().executeWithQueryId(session, query);

        long rowCount = (long) result.getResult().getMaterializedRows().get(0).getField(0);
        assertEquals(rowCount, 3000L);

        RuntimeStats runtimeStats = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId())
                .getQueryStats()
                .getRuntimeStats();

        assertTrue(sumMetric(runtimeStats, DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT) > 0);
        assertTrue(sumMetric(runtimeStats, DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT) > 0);
        assertTrue(sumMetric(runtimeStats, "externalDynamicFiltersReceived") > 0);

        // Verify dynamic filters actually pruned input rows
        long rawInputRows = sumMetric(runtimeStats, "rawInputRows");
        assertTrue(rawInputRows < 10000,
                "Dynamic filter should have pruned some input rows, but read " + rawInputRows);
    }

    @Test
    public void testDynamicFilterPushConcurrent()
            throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            futures.add(executor.submit(() -> testDynamicFilterPushToWorker()));
        }
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        executor.shutdown();
    }

    private static long sumMetric(RuntimeStats runtimeStats, String metricName)
    {
        return runtimeStats.getMetrics().entrySet().stream()
                .filter(e -> e.getKey().equals(metricName) || e.getKey().endsWith(metricName))
                .mapToLong(e -> e.getValue().getSum())
                .sum();
    }
}
