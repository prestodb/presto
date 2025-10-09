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
package com.facebook.presto.plugin.prometheus;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration tests for Prometheus with case sensitivity enabled.
 * These tests verify that when case-sensitive-name-matching is enabled,
 * tables are only accessible with their exact case.
 */
@Test(singleThreaded = true)
public class TestPrometheusCaseSensitiveIntegration
        extends AbstractTestQueryFramework
{
    // Standard metrics that are available in Prometheus
    private static final String METRIC_UP = "up"; // Standard Prometheus metric (always lowercase)
    private static final String METRIC_GO_INFO = "go_info";
    private static final String METRIC_SCRAPE_DURATION = "scrape_duration_seconds";
    private static final String METRIC_PROCESS_CPU = "process_cpu_seconds_total";

    private PrometheusServer server;
    private Session session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer();
        // Create a query runner with case sensitivity enabled
        Map<String, String> properties = ImmutableMap.of(
                "prometheus.uri", server.getUri().toString(),
                "case-sensitive-name-matching", "true");
        QueryRunner runner = createPrometheusQueryRunner(properties);
        session = runner.getDefaultSession();
        // Wait for the Prometheus server to be ready
        waitForPrometheusMetrics(runner);
        return runner;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Waits for the Prometheus server to be ready and for metrics to be available.
     */
    private void waitForPrometheusMetrics(QueryRunner runner) throws Exception
    {
        int maxTries = 60;
        int timeBetweenTriesMillis = 1000;
        int tries = 0;

        while (tries < maxTries) {
            if (session != null && runner.tableExists(session, METRIC_UP)) {
                return;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        fail("Prometheus client not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
    }

    /**
     * Helper method to assert that a table exists.
     */
    private void assertTableExists(Session session, String tableName)
    {
        assertTrue(getQueryRunner().tableExists(session, tableName),
                "Table " + tableName + " does not exist");
    }

    /**
     * Tests querying data with case sensitivity enabled.
     * When case sensitivity is enabled:
     * 1. Tables should only be accessible with their exact case
     * 2. Queries with different case variations should fail
     */
    @Test
    public void testCaseSensitiveQueries()
    {
        // 1. Test standard metrics with their exact case
        MaterializedResult upResult = getQueryRunner().execute(session,
                "SELECT * FROM prometheus.default." + METRIC_UP + " LIMIT 1").toTestTypes();
        assertEquals(upResult.getRowCount(), 1, "Query with exact case should return 1 row");

        MaterializedResult goInfoResult = getQueryRunner().execute(session,
                "SELECT * FROM prometheus.default." + METRIC_GO_INFO + " LIMIT 1").toTestTypes();
        assertEquals(goInfoResult.getRowCount(), 1, "Query with exact case should return 1 row");
        // 2. Verify that using a different case doesn't work

        // Test with uppercase variant of up
        assertQueryFails(
                "SELECT * FROM prometheus.default." + METRIC_UP.toUpperCase() + " LIMIT 1",
                "Table prometheus.default." + METRIC_UP.toUpperCase() + " does not exist");

        // Test with mixed case variant of up
        assertQueryFails(
                "SELECT * FROM prometheus.default.Up LIMIT 1",
                "Table prometheus.default.Up does not exist");

        // Test with mixed case variant of go_info
        String goInfoMixedCase = "Go_Info";
        assertQueryFails(
                "SELECT * FROM prometheus.default." + goInfoMixedCase + " LIMIT 1",
                "Table prometheus.default." + goInfoMixedCase + " does not exist");
    }

    /**
     * Tests that tables exist and are accessible with case sensitivity enabled.
     */
    @Test
    public void testTablesExist()
    {
        // Verify standard metrics are accessible with their exact case
        assertTableExists(session, METRIC_UP);
        assertTableExists(session, METRIC_GO_INFO);
        assertTableExists(session, METRIC_SCRAPE_DURATION);
        assertTableExists(session, METRIC_PROCESS_CPU);
    }

    /**
     * Tests table visibility with case sensitivity enabled.
     */
    @Test
    public void testTableVisibility()
    {
        // Get the list of tables
        MaterializedResult tablesResult = getQueryRunner().execute(session, "SHOW TABLES FROM prometheus.default").toTestTypes();
        // Verify that we can see the standard metrics
        Set<String> tableNames = tablesResult.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(Collectors.toSet());
        // Verify that standard metrics are visible
        assertTrue(tableNames.contains(METRIC_UP),
                METRIC_UP + " table not found");
        assertTrue(tableNames.contains(METRIC_GO_INFO),
                METRIC_GO_INFO + " table not found");
    }
}
