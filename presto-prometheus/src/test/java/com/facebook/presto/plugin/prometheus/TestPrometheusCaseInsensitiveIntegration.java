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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration tests for Prometheus with case sensitivity disabled.
 * These tests verify that when case-sensitive-name-matching is disabled,
 * tables are accessible with any case variation.
 */
@Test(singleThreaded = true)
public class TestPrometheusCaseInsensitiveIntegration
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
        // Create a query runner with case sensitivity disabled
        Map<String, String> properties = ImmutableMap.of(
                "prometheus.uri", server.getUri().toString(),
                "case-sensitive-name-matching", "false");
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
     * Tests that tables exist and are accessible with case sensitivity disabled.
     */
    @Test
    public void testTablesExist()
    {
        // Verify standard metrics are accessible with their original case
        assertTableExists(session, METRIC_UP);
        assertTableExists(session, METRIC_GO_INFO);
        assertTableExists(session, METRIC_SCRAPE_DURATION);
        assertTableExists(session, METRIC_PROCESS_CPU);
    }

    /**
     * Tests table visibility with case sensitivity disabled.
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
    /**
     * Tests querying data with case sensitivity disabled.
     *
     * When case sensitivity is disabled:
     * 1. Tables should be accessible with any case variation
     * 2. All case variations should refer to the same table (return the same data)
     */
    @Test
    public void testCaseInsensitiveQueries()
    {
        // Test standard metrics with different case variations
        testMetricWithDifferentCases(METRIC_UP);
        testMetricWithDifferentCases(METRIC_GO_INFO);
        testMetricWithDifferentCases(METRIC_SCRAPE_DURATION);
        testMetricWithDifferentCases(METRIC_PROCESS_CPU);
        testMetricWithDifferentCases("UP");
        testMetricWithDifferentCases("GO_INFO");
    }

    /**
     * Helper method to test a metric with different case variations.
     *
     * This method tests that a metric can be queried with different case variations
     * when case sensitivity is disabled. It verifies that:
     * 1. The query succeeds with each case variation
     * 2. Each case variation returns the same data (same value)
     */
    private void testMetricWithDifferentCases(String metricName)
    {
        // Test with original case
        MaterializedResult originalCaseResult = getQueryRunner().execute(session,
                "SELECT * FROM prometheus.default." + metricName + " LIMIT 1").toTestTypes();
        assertEquals(originalCaseResult.getRowCount(), 1, "Query with original case should return 1 row");
        // Get the value from the original case result to compare with other case variations
        Object originalValue = null;
        if (originalCaseResult.getRowCount() > 0) {
            originalValue = originalCaseResult.getMaterializedRows().get(0).getField(2); // value is in the 3rd column
        }
        // Test with uppercase
        String upperCase = metricName.toUpperCase();
        MaterializedResult upperCaseResult = getQueryRunner().execute(session,
                "SELECT * FROM prometheus.default." + upperCase + " LIMIT 1").toTestTypes();
        assertEquals(upperCaseResult.getRowCount(), 1, "Query with uppercase should return 1 row");

        // Verify that uppercase query returns the same value as original case
        if (originalValue != null && upperCaseResult.getRowCount() > 0) {
            Object upperCaseValue = upperCaseResult.getMaterializedRows().get(0).getField(2);
            assertEquals(upperCaseValue, originalValue,
                    "Uppercase query should return the same value as original case");
        }
        // Test with mixed case (capitalize first letter of each word)
        String mixedCase = Arrays.stream(metricName.split("_"))
                .map(word -> word.isEmpty() ? "" : Character.toUpperCase(word.charAt(0)) + word.substring(1).toLowerCase())
                .collect(Collectors.joining("_"));

        MaterializedResult mixedCaseResult = getQueryRunner().execute(session,
                "SELECT * FROM prometheus.default." + mixedCase + " LIMIT 1").toTestTypes();
        assertEquals(mixedCaseResult.getRowCount(), 1, "Query with mixed case should return 1 row");
        // Verify that mixed case query returns the same value as original case
        if (originalValue != null && mixedCaseResult.getRowCount() > 0) {
            Object mixedCaseValue = mixedCaseResult.getMaterializedRows().get(0).getField(2);
            assertEquals(mixedCaseValue, originalValue,
                    "Mixed case query should return the same value as original case");
        }
    }
}
