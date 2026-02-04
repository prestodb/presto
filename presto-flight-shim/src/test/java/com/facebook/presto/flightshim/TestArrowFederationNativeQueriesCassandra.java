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
import com.facebook.presto.cassandra.CassandraPlugin;
import com.facebook.presto.cassandra.CassandraServer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createKeyspace;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJavaQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestArrowFederationNativeQueriesCassandra
        extends AbstractTestArrowFederationNativeQueries
{
    private static final String CONNECTOR_ID = "cassandra";
    private static final String PLUGIN_BUNDLES = "../presto-cassandra/pom.xml";

    private final CassandraServer cassandraServer;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private FlightServer server;

    public TestArrowFederationNativeQueriesCassandra()
            throws Exception
    {
        this.cassandraServer = new CassandraServer();
        closeables.add(cassandraServer);
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
                        getConnectorProperties(cassandraServer)),
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
            queryRunner.installPlugin(new CassandraPlugin());
            queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(cassandraServer));
            createTpchTables(getSession(), cassandraServer, queryRunner);
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
                .setCatalog("cassandra")
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
        queryRunner.installPlugin(new CassandraPlugin());
        queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getConnectorProperties(cassandraServer));
        return queryRunner;
    }

    // Cassandra connector needs to create a table before inserting any rows.
    // Validate that the table isn't created as creating tables isn't supported by the flight server shim.
    @Test
    public void testTableCreation()
    {
        assertQueryFails("CREATE TABLE temp AS SELECT * FROM nation", ".*CREATE TABLE AS operations are not supported by this connecto.*r");
        assertQueryFails("SELECT * FROM temp", ".*Table cassandra.tpch.temp does not exist.*");
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    public void testJoinWithLessThanOnDatesInJoinClause()
    {
        // Cassandra does not support DATE
    }

    @Override
    public void testRenameTable()
    {
        // Cassandra does not support renaming tables
    }

    @Override
    public void testAddColumn()
    {
        // Cassandra does not support adding columns
    }

    @Override
    public void testRenameColumn()
    {
        // Cassandra does not support renaming columns
    }

    @Override
    public void testDropColumn()
    {
        // Cassandra does not support dropping columns
    }

    @Override
    public void testInsert()
    {
        // TODO Cassandra connector supports inserts, but the test would fail
    }

    @Override
    public void testCreateTable()
    {
        // Cassandra connector currently does not support create table
    }

    @Override
    public void testDelete()
    {
        // Cassandra connector currently does not support delete
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // Ignore since Cassandra connector currently does not support create table
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("custkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("orderstatus", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("totalprice", "double", "", "", Long.valueOf(53), null, null)
                .row("orderdate", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("orderpriority", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("clerk", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("shippriority", "integer", "", "", Long.valueOf(10), null, null)
                .row("comment", "varchar", "", "", null, null, Long.valueOf(2147483647))
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
    public void testWrittenStats()
    {
        // TODO Cassandra connector supports CTAS and inserts, but the test would fail
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // Connector only supports writes using ctas
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // Connector only supports writes using ctas
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testStringFilters()
    {
        // no op -- test not supported due to lack of support for char type.
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    protected String getDateExpression(String storageFormat, String columnExpression)
    {
        return "cast(" + columnExpression + " as DATE)";
    }

    static Map<String, String> getConnectorProperties(CassandraServer server)
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("cassandra.contact-points", server.getHost());
        connectorProperties.putIfAbsent("cassandra.native-protocol-port", Integer.toString(server.getPort()));
        connectorProperties.putIfAbsent("cassandra.allow-drop-table", "true");
        return ImmutableMap.copyOf(connectorProperties);
    }

    static void createTpchTables(Session session, CassandraServer server, QueryRunner queryRunner)
            throws Exception
    {
        createKeyspace(server.getSession(), "tpch");
        List<TpchTable<?>> tables = TpchTable.getTables();
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, tables);
        for (TpchTable<?> table : tables) {
            server.refreshSizeEstimates("tpch", table.getTableName());
        }
    }
}
