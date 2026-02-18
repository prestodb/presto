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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration tests for Prometheus with mixed-case metrics.
 * These tests use PrometheusServer (real Docker container) to test
 * case sensitivity with the "up" metric, which is available by default.
 */
@Test(singleThreaded = true)
public class TestPrometheusMixedCaseIntegration
        extends AbstractTestQueryFramework
{
    // Standard metrics that are available in Prometheus
    private static final String METRIC_UP = "up";
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
        // Wait for the server to be ready
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
     * Tests that tables exist and are accessible with case sensitivity enabled.
     */
    @Test
    public void testTablesExist()
    {
        assertTrue(getQueryRunner().tableExists(session, METRIC_UP),
                "up metric should be accessible");
    }

    /**
     * Tests querying data with case sensitivity enabled.
     */
    @Test
    public void testSelect()
    {
        assertQuerySucceeds("SELECT * FROM prometheus.default." + METRIC_UP + " LIMIT 1");
        assertQueryFails("SELECT * FROM prometheus.default.UP LIMIT 1",
                "Table prometheus.default.UP does not exist");
    }

    @Test
    public void testShowColumns()
    {
        assertQuerySucceeds("SHOW COLUMNS FROM prometheus.default." + METRIC_UP);
        assertQueryFails("SHOW COLUMNS FROM prometheus.default.UP",
                "line 1:1: Table 'prometheus.default.UP' does not exist");
    }

    @Test
    public void testDescribeTable()
    {
        assertQuerySucceeds("DESCRIBE prometheus.default." + METRIC_UP);
        assertQueryFails("DESCRIBE prometheus.default.UP",
                "line 1:1: Table 'prometheus.default.UP' does not exist");
    }
}
