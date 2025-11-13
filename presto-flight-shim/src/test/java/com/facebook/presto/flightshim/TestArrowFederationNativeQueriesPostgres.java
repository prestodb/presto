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
package com.facebook.presto.flightshim;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.postgresql.PostgreSqlPlugin;
import com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner;
import com.facebook.presto.plugin.postgresql.TestPostgreSqlDistributedQueries;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.QUICK_DISTINCT_LIMIT_ENABLED;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;

public class TestArrowFederationNativeQueriesPostgres
        extends TestPostgreSqlDistributedQueries
{
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpass";
    private static final String CONNECTOR_ID = "postgresql";
    private static final String PLUGIN_BUNDLES = "../presto-postgresql/pom.xml";

    private final PostgreSQLContainer<?> postgresContainer;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private FlightServer server;

    public TestArrowFederationNativeQueriesPostgres()
    {
        this.postgresContainer = new PostgreSQLContainer<>("postgres:14")
                .withDatabaseName("tpch")
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
        this.postgresContainer.start();
        closeables.add(postgresContainer);
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        if (server != null) {
            return;
        }
        server = setUpFlightServer(CONNECTOR_ID, getConnectorProperties(postgresContainer.getJdbcUrl()), PLUGIN_BUNDLES, closeables);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        for (AutoCloseable closeable : Lists.reverse(closeables)) {
            closeable.close();
        }
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgresContainer.getJdbcUrl(), ImmutableMap.of(), TpchTable.getTables());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        setUp();
        QueryRunner queryRunner =
                NativeArrowFederationConnectorUtils.createQueryRunner(CONNECTOR_ID, server.getPort());
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(postgresContainer.getJdbcUrl()));
        return queryRunner;
    }

    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
    }

    @Test
    public void testBasic()
    {
        assertQuery("select 1 + 2");
        assertQuery("select * from nation");
        assertQuery("select * from nation where nationkey between 10 and 20");
    }

    private static Map<String, String> getConnectorProperties(String jdbcUrl)
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", jdbcUrl);
        connectorProperties.putIfAbsent("connection-user", TEST_USER);
        connectorProperties.putIfAbsent("connection-password", TEST_PASSWORD);
        connectorProperties.putIfAbsent("allow-drop-table", "true");
        return ImmutableMap.copyOf(connectorProperties);
    }

    // Queries with LIMIT clause
    @Test
    public void testArrayShuffle()
    {

    }

    @Test
    public void testCorrelatedExistsSubqueries()
    {
    }

    @Test
    public void testGroupByOrderByLimit()
    {
    }

    @Test
    public void testLimitZero()
    {
    }

    @Test
    public void testLimitAll()
    {
    }

    // end LIMIT

    @Test
    public void testDereferenceInComparison()
    {
    }

    @Test
    public void testDereferenceInFunctionCall()
    {
    }

    @Test
    public void testDistinct()
    {
    }

    @Test
    public void testDistinctHaving()
    {
    }

    @Test
    public void testDistinctLimitWithQuickDistinctLimitEnabled()
    {
    }

    @Test
    public void testDistinctLimit()
    {
    }

    @Test
    public void testDistinctLimitInternal(Session session)
    {
    }

    @Test
    public void testDistinctWithOrderBy()
    {
    }

    @Test
    public void testDistinctWithOrderByNotInSelect()
    {
    }

    @Test
    public void testDistinctFrom()
    {
    }

    @Test
    public void testDistinctMultipleFields()
    {
    }

    @Test
    public void testDuplicateFields()
    {
    }
}