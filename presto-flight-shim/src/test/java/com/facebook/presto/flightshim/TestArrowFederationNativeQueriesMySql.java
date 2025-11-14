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

import com.facebook.presto.plugin.mysql.MySqlPlugin;
import com.facebook.presto.plugin.mysql.TestMySqlDistributedQueries;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.flightshim.AbstractTestFlightShimBase.addDatabaseCredentialsToJdbcUrl;
import static com.facebook.presto.flightshim.AbstractTestFlightShimBase.removeDatabaseFromJdbcUrl;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;

public class TestArrowFederationNativeQueriesMySql
        extends TestMySqlDistributedQueries
{
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpass";
    private static final String CONNECTOR_ID = "mysql";
    private static final String PLUGIN_BUNDLES = "../presto-mysql/pom.xml";

    private final MySQLContainer<?> mysqlContainer;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private FlightServer server;

    public TestArrowFederationNativeQueriesMySql()
    {
        this.mysqlContainer = new MySQLContainer<>("mysql:8.0")
                .withDatabaseName("tpch")
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
        mysqlContainer.start();
        closeables.add(mysqlContainer);
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        if (server != null) {
            return;
        }
        server = setUpFlightServer(CONNECTOR_ID, getConnectorProperties(mysqlContainer.getJdbcUrl()), PLUGIN_BUNDLES, closeables);
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
        return createMySqlQueryRunner(mysqlContainer.getJdbcUrl(), ImmutableMap.of(), TpchTable.getTables());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        setUp();
        QueryRunner queryRunner =
                NativeArrowFederationConnectorUtils.createQueryRunner(CONNECTOR_ID, server.getPort());
        queryRunner.installPlugin(new MySqlPlugin());
        queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(mysqlContainer.getJdbcUrl()));
        return queryRunner;
    }

    private static Map<String, String> getConnectorProperties(String jdbcUrl)
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", getConnectionUrl(jdbcUrl));
        connectorProperties.putIfAbsent("connection-user", TEST_USER);
        connectorProperties.putIfAbsent("connection-password", TEST_PASSWORD);
        connectorProperties.putIfAbsent("allow-drop-table", "true");
        return ImmutableMap.copyOf(connectorProperties);
    }

    private static String getConnectionUrl(String jdbcUrl)
    {
        return addDatabaseCredentialsToJdbcUrl(
                removeDatabaseFromJdbcUrl(jdbcUrl),
                TEST_USER,
                TEST_PASSWORD);
    }
}