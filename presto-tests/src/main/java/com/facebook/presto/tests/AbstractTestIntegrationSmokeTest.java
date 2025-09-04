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

import com.facebook.presto.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;

public abstract class AbstractTestIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    protected boolean isDateTypeSupported()
    {
        return true;
    }

    protected boolean isParameterizedVarcharSupported()
    {
        return true;
    }

    @Test
    public void testAggregateSingleColumn()
    {
        assertQuery("SELECT SUM(orderkey) FROM orders");
        assertQuery("SELECT SUM(totalprice) FROM orders");
        assertQuery("SELECT MAX(comment) FROM orders");
    }

    @Test
    public void testColumnsInReverseOrder()
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM orders");
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM orders");
    }

    @Test
    public void testExactPredicate()
    {
        assertQuery("SELECT * FROM orders WHERE orderkey = 10");
    }

    @Test
    public void testInListPredicate()
    {
        assertQuery("SELECT * FROM orders WHERE orderkey IN (10, 11, 20, 21)");
    }

    @Test
    public void testIsNullPredicate()
    {
        assertQuery("SELECT * FROM orders WHERE orderkey = 10 OR orderkey IS NULL");
    }

    @Test
    public void testLimit()
    {
        assertEquals(computeActual("SELECT * FROM orders LIMIT 10").getRowCount(), 10);
    }

    @Test
    public void testMultipleRangesPredicate()
    {
        assertQuery("SELECT * FROM orders WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testRangePredicate()
    {
        assertQuery("SELECT * FROM orders WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testSelectAll()
    {
        assertQuery("SELECT * FROM orders");
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toTestTypes();

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row(getQueryRunner().getDefaultSession().getSchema().orElse("tpch"));

        assertContains(actualSchemas, resultBuilder.build());
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult actualTables = computeActual("SHOW TABLES").toTestTypes();
        MaterializedResult expectedTables = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row("orders")
                .build();
        assertContains(actualTables, expectedTables);
    }

    @Test
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual("DESC orders").toTestTypes();
        assertEquals(actualColumns, getExpectedOrdersTableDescription());
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll("^.", "_");

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schema + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE '" + schema + "' AND table_name LIKE '%orders'",
                "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'",
                ordersTableWithColumns);
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
    }

    @Test
    public void testDuplicatedRowCreateTable()
    {
        assertQueryFails("CREATE TABLE test (a integer, a integer)",
                "line 1:31: Column name 'a' specified more than once");
        assertQueryFails("CREATE TABLE test (a integer, orderkey integer, LIKE orders INCLUDING PROPERTIES)",
                "line 1:49: Column name 'orderkey' specified more than once");

        assertQueryFails("CREATE TABLE test (a integer, A integer)",
                "line 1:31: Column name 'A' specified more than once");
        assertQueryFails("CREATE TABLE test (a integer, OrderKey integer, LIKE orders INCLUDING PROPERTIES)",
                "line 1:49: Column name 'orderkey' specified more than once");
    }

    protected MaterializedResult getExpectedOrdersTableDescription()
    {
        String orderDateType;
        if (isDateTypeSupported()) {
            orderDateType = "date";
        }
        else {
            orderDateType = "varchar";
        }
        if (isParameterizedVarcharSupported()) {
            return MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                    .row("orderkey", "bigint", "", "", 19L, null, null)
                    .row("custkey", "bigint", "", "", 19L, null, null)
                    .row("orderstatus", "varchar(1)", "", "", null, null, 1L)
                    .row("totalprice", "double", "", "", 53L, null, null)
                    .row("orderdate", orderDateType, "", "", null, null, "varchar".equals(2147483647L) ? 2147483647L : null)
                    .row("orderpriority", "varchar(15)", "", "", null, null, 15L)
                    .row("clerk", "varchar(15)", "", "", null, null, 15L)
                    .row("shippriority", "integer", "", "", 10L, null, null)
                    .row("comment", "varchar(79)", "", "", null, null, 79L)
                    .build();
        }
        else {
            return MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                    .row("orderkey", "bigint", "", "", 19L, null, null)
                    .row("custkey", "bigint", "", "", 19L, null, null)
                    .row("orderstatus", "varchar", "", "", null, null, 2147483647L)
                    .row("totalprice", "double", "", "", 53L, null, null)
                    .row("orderdate", orderDateType, "", "", null, null, "varchar".equals(orderDateType) ? 2147483647L : null)
                    .row("orderpriority", "varchar", "", "", null, null, 2147483647L)
                    .row("clerk", "varchar", "", "", null, null, 2147483647L)
                    .row("shippriority", "integer", "", "", 10L, null, null)
                    .row("comment", "varchar", "", "", null, null, 2147483647L)
                    .build();
        }
    }
}
