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
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJavaQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.getConnectorProperties;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner.createSchema;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestArrowFederationNativeQueriesPostgres
        extends AbstractTestArrowFederationNativeQueries
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
        server = setUpFlightServer(ImmutableMap.of(CONNECTOR_ID, postgresContainer.getJdbcUrl()), PLUGIN_BUNDLES, closeables);
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
            queryRunner.installPlugin(new PostgreSqlPlugin());
            queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(postgresContainer.getJdbcUrl()));
            createTpchTables(queryRunner, postgresContainer.getJdbcUrl());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("postgresql")
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
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(postgresContainer.getJdbcUrl()));
        return queryRunner;
    }

    @Override
    public void testInsert()
    {
        // no op -- test not supported due to lack of support for array types.  See
        // TestPostgreSqlIntegrationSmokeTest for insertion tests.
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
        // PostgreSQL does not support MAP type
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // PostgreSQL does not support MAP type
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // PostgreSQL does not support MAP type
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // PostgreSQL does not support ROW type
    }

    public static void createTpchTables(QueryRunner queryRunner, String postgresJdbcUrl)
    {
        // create schema for postgresQuery Runner
        try {
            createSchema(postgresJdbcUrl, "tpch", TEST_USER, TEST_PASSWORD);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        copyTpchTables(
                queryRunner,
                "tpch",
                TINY_SCHEMA_NAME,
                testSessionBuilder()
                        .setCatalog("postgresql")
                        .setSchema("tpch")
                        .build(),
                TpchTable.getTables());
    }
}
