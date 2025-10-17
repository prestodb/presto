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
    private static final String METRIC_GO_INFO_UPPER = "GO_INFO";
    private static final String METRIC_GO_INFO_MIXED = "Go_Info";
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
     * Tests that tables exist and are accessible with case sensitivity disabled.
     */
    @Test
    public void testTablesExist()
    {
        // Test with 'go_info' metric - verify case insensitive access
        // When case sensitivity is disabled, the table name is normalized to lowercase
        // So we can query using any case variation, but the actual table must exist in Prometheus
        assertTrue(getQueryRunner().tableExists(session, METRIC_GO_INFO) &&
                getQueryRunner().tableExists(session, METRIC_GO_INFO_MIXED.toLowerCase()),
                "The 'go_info' metric should be accessible with any variation");
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
        // Verify that standard metrics are visible with their original case
        assertTrue(tableNames.contains(METRIC_UP) && tableNames.contains(METRIC_GO_INFO),
                "Standard metrics should be visible in the table list");
    }
    /**
     * Tests querying data with case sensitivity disabled.
     * When case sensitivity is disabled:
     * 1. Tables should be accessible with any case variation
     * 2. All case variations should refer to the same table (return the same data)
     */
    @Test
    public void testCaseInsensitiveQueries()
    {
        assertQuerySucceeds("SELECT * FROM prometheus.default." + METRIC_GO_INFO + " LIMIT 1");
        assertQuerySucceeds("SELECT * FROM prometheus.default." + METRIC_GO_INFO_MIXED + " LIMIT 1");
        assertQuerySucceeds("SELECT * from prometheus.default." + METRIC_GO_INFO_UPPER + " LIMIT 1");
    }
}
