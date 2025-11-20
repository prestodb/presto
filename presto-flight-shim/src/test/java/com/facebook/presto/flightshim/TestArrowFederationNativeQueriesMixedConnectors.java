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
import com.facebook.presto.plugin.postgresql.PostgreSqlPlugin;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.FlightServer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createJavaQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.getConnectorProperties;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.flightshim.TestArrowFederationNativeQueriesMySql.getConnectionUrl;

public class TestArrowFederationNativeQueriesMixedConnectors
        extends AbstractTestQueryFramework
{
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpass";

    private static final String POSTGRES_CONNECTOR_ID = "postgresql";
    private static final String MYSQL_CONNECTOR_ID = "mysql";
    private static final String PLUGIN_BUNDLES = "../presto-postgresql/pom.xml,\n../presto-mysql/pom.xml";

    private final PostgreSQLContainer<?> postgresContainer;
    private final MySQLContainer<?> mysqlContainer;

    private final List<AutoCloseable> closeables = new ArrayList<>();
    private FlightServer server;

    public TestArrowFederationNativeQueriesMixedConnectors()
    {
        // setup postgres
        this.postgresContainer = new PostgreSQLContainer<>("postgres:14")
                .withDatabaseName("tpch")
                .withUsername(TEST_USER)
                .withPassword(TEST_PASSWORD);
        this.postgresContainer.start();
        closeables.add(postgresContainer);

        // setup mySql
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
                        MYSQL_CONNECTOR_ID, getConnectionUrl(mysqlContainer.getJdbcUrl()),
                        POSTGRES_CONNECTOR_ID, postgresContainer.getJdbcUrl()),
                PLUGIN_BUNDLES, closeables);
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
        createTables((QueryRunner) getExpectedQueryRunner(), postgresContainer.getJdbcUrl());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        setUp();
        QueryRunner queryRunner =
                createNativeQueryRunner(
                        ImmutableList.of(MYSQL_CONNECTOR_ID, POSTGRES_CONNECTOR_ID), server.getPort());

        installPlugins(queryRunner, postgresContainer.getJdbcUrl(), mysqlContainer.getJdbcUrl());
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = createJavaQueryRunner();
        installPlugins(queryRunner, postgresContainer.getJdbcUrl(), mysqlContainer.getJdbcUrl());
        return queryRunner;
    }

    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
    }

    @Test
    public void testBasicSanityTests()
    {
        assertQuery("select * from mysql.tpch.customer");
        assertQuery("select * from postgresql.tpch.orders");
        assertQuery("select orderkey, custkey from postgresql.tpch.orders");
        assertQuery("select extendedprice * quantity from postgresql.tpch.lineitem");
        assertQuery("select name from mysql.tpch.part where name like '%steel%'");
        assertQuery("select name from mysql.tpch.part where LENGTH(name) > 10");
    }

    @Test
    public void testJoins()
    {
        assertQuery("SELECT c.custkey, o.orderkey " +
                "FROM mysql.tpch.customer c " +
                "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                "WHERE c.custkey BETWEEN 100 AND 200");
        assertQuery("SELECT p.partkey, ps.suppkey " +
                "FROM postgresql.tpch.part p " +
                "JOIN mysql.tpch.partsupp ps ON p.partkey = ps.partkey " +
                "WHERE ps.supplycost > 100 " +
                "ORDER BY p.partkey, ps.suppkey " +
                "LIMIT 100");
        assertQuery(
                "SELECT c.custkey, n.regionkey, o.orderkey, l.quantity " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN mysql.tpch.nation n ON c.nationkey =  n.nationkey " +
                        "JOIN postgresql.tpch.lineitem l ON o.orderkey = l.orderkey");
        assertQuery(
                "SELECT c.custkey " +
                        "FROM mysql.tpch.customer c " +
                        "WHERE NOT EXISTS ( " +
                        "   SELECT 1 " +
                        "   FROM postgresql.tpch.orders o " +
                        "   WHERE o.custkey = c.custkey)");
        assertQuery(
                "SELECT DISTINCT c.custkey " +
                        "FROM mysql.tpch.customer c " +
                        "WHERE c.custkey IN ( " +
                        "   SELECT custkey " +
                        "   FROM postgresql.tpch.orders o)");

        // wide JOIN
        assertQuery("SELECT * " +
                "FROM mysql.tpch.lineitem l " +
                "JOIN postgresql.tpch.orders o ON l.orderkey = o.orderkey " +
                "ORDER BY l.orderkey, o.orderkey");
    }

    @Test
    public void testLargeQueries()
    {
        // tpch q3
        assertQuery(
                "SELECT l.orderkey, sum(l.extendedprice * (1 - l.discount)) as revenue, o.orderdate, o.shippriority " +
                        "FROM " +
                        "mysql.tpch.customer as c," +
                        "postgresql.tpch.orders as o," +
                        "mysql.tpch.lineitem as l " +
                        "WHERE " +
                        "c.mktsegment = 'BUILDING' " +
                        "AND c.custkey = o.custkey " +
                        "AND l.orderkey = o.orderkey " +
                        "AND o.orderdate < DATE '1995-03-15' " +
                        "AND l.shipdate > DATE '1995-03-15' " +
                        "GROUP BY l.orderkey,o.orderdate, o.shippriority " +
                        "ORDER BY revenue desc, o.orderdate " +
                        "LIMIT 10");

        assertQuery(
                "SELECT n.name, sum(l.extendedprice * (1 - l.discount)) as revenue " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN mysql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN postgresql.tpch.lineitem l ON o.orderkey = l.orderkey " +
                        "JOIN postgresql.tpch.supplier s ON l.suppkey = s.suppkey " +
                        "JOIN mysql.tpch.nation n ON c.nationkey = n.nationkey " +
                        "GROUP BY 1 " +
                        "ORDER BY revenue DESC " +
                        "LIMIT 20");
    }

    @Test
    public void testGroupByHaving()
    {
        assertQuery(
                "SELECT o.custkey, COUNT(*) AS cnt " +
                        "FROM postgresql.tpch.orders o " +
                        "JOIN mysql.tpch.lineitem l ON o.orderkey = l.orderkey " +
                        "GROUP BY o.custkey " +
                        "HAVING COUNT(*) > 10 " +
                        "ORDER BY cnt DESC");
    }

    @Test
    public void testMultiAggregation()
    {
        assertQuery(
                "SELECT c.nationkey, COUNT(*) as orders, SUM(l.quantity) as qty, AVG(l.discount) as avg " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN mysql.tpch.lineitem l ON o.orderkey =  l.orderkey " +
                        "GROUP BY 1");
    }

    @Test
    public void testWindowFunctions()
    {
        assertQuery(
                "SELECT o.orderkey, c.custkey, " +
                        "RANK() OVER (PARTITION BY c.custkey ORDER BY o.totalprice DESC) AS rnk " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "ORDER BY o.orderkey, c.custkey " +
                        "LIMIT 100");
        assertQuery(
                "SELECT c.nationkey, l.orderkey, " +
                        "SUM(l.quantity) OVER (PARTITION BY c.nationkey ORDER BY l.orderkey) AS qty " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "JOIN mysql.tpch.lineitem l ON o.orderkey = l.orderkey");
        assertQuery("SELECT l.orderkey, l.quantity, " +
                "DENSE_RANK() OVER (ORDER BY l.quantity DESC) AS dr " +
                "FROM postgresql.tpch.lineitem l");
    }

    @Test
    public void testNullCases()
    {
        assertQuery(
                "SELECT * FROM mysql.tpch.part p " +
                        "LEFT JOIN postgresql.tpch.partsupp s ON p.partkey = s.partkey " +
                        "WHERE s.suppkey is NULL");
        assertQuery(
                "SELECT COALESCE(c.name, 'UNKNOWN') AS customer_name, o.orderkey " +
                        "FROM mysql.tpch.customer c " +
                        "JOIN postgresql.tpch.orders o ON c.custkey = o.custkey " +
                        "ORDER BY o.orderkey, customer_name");
    }

    @Test
    public void testSetOperators()
    {
        assertQuery(
                "SELECT nationkey FROM mysql.tpch.nation " +
                        "UNION ALL " +
                        "SELECT nationkey FROM postgresql.tpch.nation");
        assertQuery(
                "SELECT orderkey FROM mysql.tpch.orders " +
                        "INTERSECT " +
                        "SELECT orderkey FROM postgresql.tpch.orders");
        assertQuery(
                "SELECT custkey FROM mysql.tpch.customer " +
                        "EXCEPT " +
                        "SELECT custkey FROM postgresql.tpch.customer");
    }

    @Test(timeOut = 30_000)
    public void testRepeatedJoins()
    {
        for (int i = 0; i < 20; i++) {
            assertQuery(
                    "SELECT COUNT(*) " +
                            "FROM mysql.tpch.lineitem l " +
                            "JOIN postgresql.tpch.orders o ON l.orderkey = o.orderkey");
        }
    }

    private static void installPlugins(QueryRunner queryRunner, String postgresJdbcUrl, String mySqlJdbcUrl)
    {
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog(POSTGRES_CONNECTOR_ID, POSTGRES_CONNECTOR_ID, getConnectorProperties(postgresJdbcUrl));

        queryRunner.installPlugin(new MySqlPlugin());
        queryRunner.createCatalog(MYSQL_CONNECTOR_ID, MYSQL_CONNECTOR_ID, getConnectorProperties(getConnectionUrl(mySqlJdbcUrl)));
    }

    private static void createTables(QueryRunner queryRunner, String postgresJdbcUrl)
    {
        TestArrowFederationNativeQueriesPostgres.createTpchTables(queryRunner, postgresJdbcUrl);
        TestArrowFederationNativeQueriesMySql.createTpchTables(queryRunner);
    }
}
