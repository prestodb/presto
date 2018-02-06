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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestLocalQueries
        extends AbstractTestQueries
{
    public TestLocalQueries()
    {
        super(TestLocalQueries::createLocalQueryRunner);
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testShowColumnStats()
    {
        // FIXME Add tests for more complex scenario with more stats
        MaterializedResult result = computeActual("SHOW STATS FOR nation");

        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("regionkey", null, 5.0, 0.0, null, "0", "4")
                        .row("name", null, 25.0, 0.0, null, "ALGERIA", "VIETNAM")
                        .row("comment", null, 25.0, 0.0, null, " haggle. carefully final deposit...", "y final packages. slow foxes caj...")
                        .row("nationkey", null, 25.0, 0.0, null, "0", "24")
                        .row(null, null, null, null, 25.0, null, null)
                        .build();

        assertEquals(result, expectedStatistics);
    }

    @Test
    public void testRejectStarQueryWithoutFromRelation()
    {
        assertQueryFails("SELECT *", "line \\S+ SELECT \\* not allowed in queries without FROM clause");
        assertQueryFails("SELECT 1, '2', *", "line \\S+ SELECT \\* not allowed in queries without FROM clause");
    }

    @Test
    public void testDecimal()
    {
        assertQuery("SELECT DECIMAL '1.0'", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT DECIMAL '1.'", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT DECIMAL '0.1'", "SELECT CAST('0.1' AS DECIMAL)");
        assertQuery("SELECT 1.0", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT 1.", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT 0.1", "SELECT CAST('0.1' AS DECIMAL)");
    }
}
