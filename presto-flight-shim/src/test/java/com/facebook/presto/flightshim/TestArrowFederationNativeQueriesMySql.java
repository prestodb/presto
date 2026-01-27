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
import com.facebook.presto.plugin.mysql.MySqlPlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.flightshim.AbstractTestFlightShimBase.addDatabaseCredentialsToJdbcUrl;
import static com.facebook.presto.flightshim.AbstractTestFlightShimBase.removeDatabaseFromJdbcUrl;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJavaQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.getConnectorProperties;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestArrowFederationNativeQueriesMySql
        extends AbstractTestArrowFederationNativeQueries
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
        server = setUpFlightServer(
                ImmutableMap.of(
                        CONNECTOR_ID,
                        getConnectorProperties(getConnectionUrl(mysqlContainer.getJdbcUrl()))),
                PLUGIN_BUNDLES,
                closeables);
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
    protected void createTables()
    {
        // hack: need the java query runner to generate tables
        try {
            QueryRunner queryRunner = createJavaQueryRunner();
            queryRunner.installPlugin(new MySqlPlugin());
            queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(getConnectionUrl(mysqlContainer.getJdbcUrl())));
            createTpchTables(queryRunner);
            queryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("tpch")
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        setUp();
        QueryRunner queryRunner =
                createNativeQueryRunner(ImmutableList.of(CONNECTOR_ID), server.getPort());
        queryRunner.installPlugin(new MySqlPlugin());
        queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(getConnectionUrl(mysqlContainer.getJdbcUrl())));
        return queryRunner;
    }

    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders").toTestTypes();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(255)", "", "", null, null, 255L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "date", "", "", null, null, null)
                .row("orderpriority", "varchar(255)", "", "", null, null, 255L)
                .row("clerk", "varchar(255)", "", "", null, null, 255L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar(255)", "", "", null, null, 255L)
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testDescribeOutput()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testInsert()
    {
        // no op -- test not supported due to lack of support for array types.  See
        // TestMySqlIntegrationSmokeTest for insertion tests.
    }

    @Override
    public void testDelete()
    {
        // Delete is currently unsupported
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // JDBC connectors do not support multi-statement writes within transactions
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // JDBC connectors do not support multi-statement writes within transactions
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // JDBC connectors do not support multi-statement writes within transactions
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // MySQL does not support MAP type
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // MySQL does not support MAP type
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // MySQL does not support MAP type
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // MySQL does not support ROW type
    }

    @Override
    public void testStringFilters()
    {
        // MySQL maps char types to varchar(255), causing type mismatches
    }

    public static String getConnectionUrl(String jdbcUrl)
    {
        return addDatabaseCredentialsToJdbcUrl(
                removeDatabaseFromJdbcUrl(jdbcUrl),
                TEST_USER,
                TEST_PASSWORD);
    }

    static void createTpchTables(QueryRunner queryRunner)
    {
        copyTpchTables(
                queryRunner,
                "tpch",
                TINY_SCHEMA_NAME,
                testSessionBuilder()
                        .setCatalog("mysql")
                        .setSchema("tpch")
                        .build(),
                TpchTable.getTables());
    }
}
