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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Comprehensive test suite for case-sensitive name matching mode.
 */
@Test(singleThreaded = true)
public class TestPrometheusCaseSensitiveNameMatching
        extends AbstractTestQueryFramework
{
    private static final String METRIC_NAME = "up";
    private PrometheusServer server;
    private Session session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new PrometheusServer();
        // Enable case-sensitive mode
        Map<String, String> properties = ImmutableMap.of(
                "prometheus.uri", server.getUri().toString(),
                "case-sensitive-name-matching", "true");
        QueryRunner runner = createPrometheusQueryRunner(properties);
        session = runner.getDefaultSession();
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

    private void waitForPrometheusMetrics(QueryRunner runner)
            throws Exception
    {
        int maxTries = 60;
        int timeBetweenTriesMillis = 1000;
        int tries = 0;

        while (tries < maxTries) {
            if (session != null && runner.tableExists(session, METRIC_NAME)) {
                return;
            }
            Thread.sleep(timeBetweenTriesMillis);
            tries++;
        }
        fail("Prometheus client not available for metrics query in " + maxTries * timeBetweenTriesMillis + " milliseconds.");
    }

    /**
     * Provides case variations that should FAIL in case-sensitive mode.
     */
    @DataProvider(name = "invalidCaseVariations")
    public Object[][] invalidCaseVariations()
    {
        return new Object[][] {
                {"UP", "prometheus.default.UP"},
                {"Up", "prometheus.default.Up"},
                {"uP", "prometheus.default.uP"},
                {"UP", "prometheus.default.UP"},
        };
    }

    @Test
    public void testTableExistsWithExactCase()
    {
        // The exact case "up" should work
        assertTrue(getQueryRunner().tableExists(session, METRIC_NAME),
                "Metric 'up' should be accessible with exact case match");
    }

    @Test
    public void testSelectWithExactCase()
    {
        assertQuerySucceeds(String.format("SELECT * FROM prometheus.default.%s LIMIT 1", METRIC_NAME));
    }

    @Test
    public void testShowColumnsWithExactCase()
    {
        assertQuerySucceeds(String.format("SHOW COLUMNS FROM prometheus.default.%s", METRIC_NAME));
    }

    @Test
    public void testDescribeWithExactCase()
    {
        assertQuerySucceeds(String.format("DESCRIBE prometheus.default.%s", METRIC_NAME));
    }

    @Test(dataProvider = "invalidCaseVariations")
    public void testSelectFailsWithWrongCase(String metricVariation, String expectedTableName)
    {
        assertQueryFails(
                String.format("SELECT * FROM prometheus.default.%s LIMIT 1", metricVariation),
                String.format(".*Table %s does not exist.*", expectedTableName));
    }

    @Test(dataProvider = "invalidCaseVariations")
    public void testShowColumnsFailsWithWrongCase(String metricVariation, String expectedTableName)
    {
        assertQueryFails(
                String.format("SHOW COLUMNS FROM prometheus.default.%s", metricVariation),
                String.format(".*Table '%s' does not exist.*", expectedTableName));
    }

    @Test(dataProvider = "invalidCaseVariations")
    public void testDescribeFailsWithWrongCase(String metricVariation, String expectedTableName)
    {
        assertQueryFails(
                String.format("DESCRIBE prometheus.default.%s", metricVariation),
                String.format(".*Table '%s' does not exist.*", expectedTableName));
    }

    @Test
    public void testWrongCaseTablesDoNotExist()
    {
        assertQueryFails("SELECT * FROM prometheus.default.UP LIMIT 1",
                ".*Table prometheus.default.UP does not exist.*");
        assertQueryFails("SELECT * FROM prometheus.default.Up LIMIT 1",
                ".*Table prometheus.default.Up does not exist.*");
        assertQueryFails("SELECT * FROM prometheus.default.uP LIMIT 1",
                ".*Table prometheus.default.uP does not exist.*");
    }

    @Test
    public void testCountFailsWithWrongCase()
    {
        assertQueryFails("SELECT COUNT(*) FROM prometheus.default.UP",
                ".*Table prometheus.default.UP does not exist.*");
        assertQueryFails("SELECT COUNT(*) FROM prometheus.default.Up",
                ".*Table prometheus.default.Up does not exist.*");
    }

    @Test
    public void testAggregationFailsWithWrongCase()
    {
        assertQueryFails("SELECT MAX(value) FROM prometheus.default.UP",
                ".*Table prometheus.default.UP does not exist.*");
    }

    protected void assertQuerySucceeds(String query)
    {
        try {
            getQueryRunner().execute(session, query);
        }
        catch (Exception e) {
            fail(String.format("Query should have succeeded but failed: %s. Error: %s", query, e.getMessage()), e);
        }
    }
}
