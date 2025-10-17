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
import static org.testng.Assert.assertFalse;
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
    private static final String METRIC_GO_INFO_UPPER = "GO_INFO";
    private static final String METRIC_GO_INFO_MIXED = "Go_Info";
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
     * Tests querying data with case sensitivity enabled.
     * When case sensitivity is enabled:
     * 1. Tables should only be accessible with their exact case
     * 2. Queries with different case variations should fail
     */
    @Test
    public void testCaseSensitiveQueries()
    {
        assertQuerySucceeds("SELECT * FROM prometheus.default." + METRIC_GO_INFO + " LIMIT 1");
        assertQueryFails("SELECT * FROM prometheus.default." + METRIC_GO_INFO_UPPER + " LIMIT 1",
                "Table prometheus.default." + METRIC_GO_INFO_UPPER + " does not exist");
        assertQueryFails("SELECT * FROM prometheus.default." + METRIC_GO_INFO_MIXED + " LIMIT 1",
                "Table prometheus.default." + METRIC_GO_INFO_MIXED + " does not exist");
    }

    /**
     * Tests that tables exist and are accessible with case sensitivity enabled.
     */
    @Test
    public void testTablesExist()
    {
        // Verify metrics are accessible with their exact case
        assertTrue(getQueryRunner().tableExists(session, METRIC_GO_INFO),
                "go_info metric should be accessible");
        assertFalse(getQueryRunner().tableExists(session, METRIC_GO_INFO_MIXED),
                "Go_Info metric should not be accessible");
        assertFalse(getQueryRunner().tableExists(session, METRIC_GO_INFO_UPPER),
                "GO_INFO metric should not be accessible");
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
        // Verify that go_info is visible, but Go_Info and GO_INFO are not
        assertTrue(tableNames.contains(METRIC_GO_INFO) &&
                !tableNames.contains(METRIC_GO_INFO_MIXED) &&
                !tableNames.contains(METRIC_GO_INFO_UPPER),
                "Only go_info should be visible in the table list, not Go_Info or GO_INFO");
    }
}
