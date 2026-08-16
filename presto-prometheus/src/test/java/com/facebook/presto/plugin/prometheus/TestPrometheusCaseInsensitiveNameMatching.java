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
import static org.testng.Assert.fail;

/**
 * Comprehensive test suite for the case-insensitive-name-matching configuration flag.
 */
@Test(singleThreaded = true)
public class TestPrometheusCaseInsensitiveNameMatching
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
        // Default behavior: case-sensitive-name-matching = false (case-insensitive mode)
        Map<String, String> properties = ImmutableMap.of(
                "prometheus.uri", server.getUri().toString(),
                "case-sensitive-name-matching", "false");
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
     * Provides various case variations of the "up" metric name for parameterized testing.
     */
    @DataProvider(name = "caseVariations")
    public Object[][] caseVariations()
    {
        return new Object[][] {
                {"up"},
                {"UP"},
                {"Up"},
                {"uP"},
        };
    }

    @Test(dataProvider = "caseVariations")
    public void testSelectWithAnyCaseVariation(String metricVariation)
    {
        // All case variations should work in case-insensitive mode
        assertQuerySucceeds(String.format("SELECT * FROM prometheus.default.%s LIMIT 1", metricVariation));
    }

    @Test(dataProvider = "caseVariations")
    public void testShowColumnsWithAnyCaseVariation(String metricVariation)
    {
        assertQuerySucceeds(String.format("SHOW COLUMNS FROM prometheus.default.%s", metricVariation));
    }

    @Test(dataProvider = "caseVariations")
    public void testDescribeWithAnyCaseVariation(String metricVariation)
    {
        assertQuerySucceeds(String.format("DESCRIBE prometheus.default.%s", metricVariation));
    }

    @Test(dataProvider = "caseVariations")
    public void testCountWithAnyCaseVariation(String metricVariation)
    {
        assertQuerySucceeds(String.format("SELECT COUNT(*) FROM prometheus.default.%s", metricVariation));
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
