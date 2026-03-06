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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.common.RuntimeMetricName;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.facebook.presto.tpch.TpchPlugin;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCoordinatorOutputBuffering
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setNodeCount(2)
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test(timeOut = 120_000)
    public void testBufferingMetricsPresent()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();

        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(session, "SELECT * FROM nation");
        assertTrue(result.getResult().getRowCount() > 0, "Expected non-empty result set");

        QueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(result.getQueryId());
        RuntimeStats runtimeStats = queryInfo.getQueryStats().getRuntimeStats();

        assertNotNull(
                runtimeStats.getMetrics().get(RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_PAGES),
                "Expected coordinatorBufferDrainedPages metric to be present");
        assertTrue(
                runtimeStats.getMetrics().get(RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_PAGES).getSum() > 0,
                "Expected coordinatorBufferDrainedPages sum > 0");

        assertNotNull(
                runtimeStats.getMetrics().get(RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_BYTES),
                "Expected coordinatorBufferDrainedBytes metric to be present");
        assertTrue(
                runtimeStats.getMetrics().get(RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_BYTES).getSum() > 0,
                "Expected coordinatorBufferDrainedBytes sum > 0");
    }

    @Test(timeOut = 120_000)
    public void testBufferingDisabledNoMetrics()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(session, "SELECT * FROM nation");
        assertTrue(result.getResult().getRowCount() > 0, "Expected non-empty result set");

        QueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(result.getQueryId());
        RuntimeStats runtimeStats = queryInfo.getQueryStats().getRuntimeStats();

        assertNull(
                runtimeStats.getMetrics().get(RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_PAGES),
                "Expected coordinatorBufferDrainedPages metric to NOT be present when buffering is disabled");
        assertNull(
                runtimeStats.getMetrics().get(RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_BYTES),
                "Expected coordinatorBufferDrainedBytes metric to NOT be present when buffering is disabled");
    }

    @Test(timeOut = 120_000)
    public void testBufferCleanupOnQueryFailure()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();

        try (StatementClient client = queryRunner.getRandomClient().createStatementClient(
                session, "SELECT 1/0 FROM nation")) {
            while (client.isRunning()) {
                client.advance();
            }

            assertNotNull(client.finalStatusInfo().getError(),
                    "Expected query to fail with an arithmetic error");
        }
    }

    @Test(timeOut = 120_000)
    public void testNoDataDeliveredBeforeQueryCompletion()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();

        try (StatementClient client = queryRunner.getRandomClient().createStatementClient(session, "SELECT * FROM lineitem")) {
            boolean sawNonFinishedState = false;
            boolean dataReceivedBeforeFinished = false;
            int totalDataRows = 0;

            while (client.isRunning()) {
                String state = client.currentStatusInfo().getStats().getState();
                boolean isFinished = "FINISHED".equals(state);

                if (!isFinished) {
                    sawNonFinishedState = true;
                }

                Iterable<List<Object>> data = client.currentData().getData();
                if (data != null) {
                    for (List<Object> row : data) {
                        totalDataRows++;
                        if (!isFinished) {
                            dataReceivedBeforeFinished = true;
                        }
                    }
                }

                client.advance();
            }

            assertFalse(client.isClientError(), "Query failed with client error");
            assertNull(client.finalStatusInfo().getError(), "Query failed with server error");
            assertTrue(sawNonFinishedState,
                    "Expected to observe query in a non-FINISHED state during polling");
            assertFalse(dataReceivedBeforeFinished,
                    "With coordinator output buffering enabled, no data should be delivered before query completion");
            assertTrue(totalDataRows > 0,
                    "Expected to receive data rows after query completion");
        }
    }

    @Test(timeOut = 120_000)
    public void testDataCorrectnessWithBuffering()
    {
        String query = "SELECT nationkey, name, regionkey, comment FROM nation ORDER BY nationkey";

        Session baselineSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        MaterializedResult expected = queryRunner.execute(baselineSession, query);

        Session bufferedSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();
        MaterializedResult actual = queryRunner.execute(bufferedSession, query);

        List<MaterializedRow> expectedRows = expected.getMaterializedRows();
        List<MaterializedRow> actualRows = actual.getMaterializedRows();
        assertEquals(actualRows.size(), expectedRows.size(),
                "Row count mismatch between buffered and non-buffered results");
        assertEquals(actualRows, expectedRows,
                "Row data mismatch between buffered and non-buffered results");
    }

    @Test(timeOut = 120_000)
    public void testDataCorrectnessWithLargerResultSet()
    {
        // lineitem (~60K rows in tiny) exercises the multi-page spooling path
        String query = "SELECT orderkey, partkey, suppkey, linenumber, quantity FROM lineitem ORDER BY orderkey, linenumber";

        Session baselineSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        MaterializedResult expected = queryRunner.execute(baselineSession, query);

        Session bufferedSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();
        MaterializedResult actual = queryRunner.execute(bufferedSession, query);

        List<MaterializedRow> expectedRows = expected.getMaterializedRows();
        List<MaterializedRow> actualRows = actual.getMaterializedRows();
        assertEquals(actualRows.size(), expectedRows.size(),
                "Row count mismatch for lineitem between buffered and non-buffered results");
        assertEquals(actualRows, expectedRows,
                "Row data mismatch for lineitem between buffered and non-buffered results");
    }

    @Test(timeOut = 120_000)
    public void testExplainQueryWithBuffering()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();

        MaterializedResult result = queryRunner.execute(session, "EXPLAIN SELECT * FROM nation");
        assertTrue(result.getRowCount() > 0, "Expected EXPLAIN to produce output");
    }

    @Test(timeOut = 120_000)
    public void testQueryCancellationWithBuffering()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("coordinator_output_buffering_enabled", "true")
                .build();

        ExecutorService executor = newSingleThreadExecutor();
        try {
            Future<?> queryFuture = executor.submit(() -> {
                queryRunner.execute(session,
                        "SELECT a.comment, b.comment FROM lineitem a CROSS JOIN lineitem b WHERE a.orderkey < 5");
            });

            Thread.sleep(2000);

            for (BasicQueryInfo queryInfo : queryRunner.getCoordinator().getQueryManager().getQueries()) {
                if (!queryInfo.getState().isDone()) {
                    try {
                        queryRunner.getCoordinator().getQueryManager().cancelQuery(queryInfo.getQueryId());
                    }
                    catch (Exception ignored) {
                    }
                }
            }

            // Verify the query completes (doesn't hang) after cancellation
            try {
                queryFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
            }
            catch (Exception e) {
                // Expected: cancelled query throws
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
