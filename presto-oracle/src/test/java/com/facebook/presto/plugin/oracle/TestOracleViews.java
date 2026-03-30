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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test cases for Oracle view support.
 * Tests CREATE VIEW, DROP VIEW, SELECT from views, and SHOW CREATE VIEW operations.
 */
public class TestOracleViews
        extends AbstractTestQueryFramework
{
    private static OracleServerTester oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new OracleServerTester();
        return createOracleQueryRunner(oracleServer, ORDERS);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Test
    public void testCreateAndQuerySimpleView()
    {
        assertUpdate("CREATE VIEW test_simple_view AS SELECT orderkey, orderstatus FROM orders");

        assertQuery(
                "SELECT * FROM test_simple_view WHERE orderkey < 100",
                "SELECT orderkey, orderstatus FROM orders WHERE orderkey < 100");

        assertUpdate("DROP VIEW test_simple_view");
    }

    @Test
    public void testCreateOrReplaceView()
    {
        // Create initial view
        assertUpdate("CREATE VIEW test_replace_view AS SELECT orderkey FROM orders");

        // Verify initial view
        assertQuery(
                "SELECT * FROM test_replace_view WHERE orderkey < 10",
                "SELECT orderkey FROM orders WHERE orderkey < 10");

        // Replace with different definition
        assertUpdate("CREATE OR REPLACE VIEW test_replace_view AS SELECT orderkey, orderstatus, totalprice FROM orders");

        // Verify replaced view has new columns
        assertQuery(
                "SELECT * FROM test_replace_view WHERE orderkey < 10",
                "SELECT orderkey, orderstatus, totalprice FROM orders WHERE orderkey < 10");

        assertUpdate("DROP VIEW test_replace_view");
    }

    @Test
    public void testViewWithSelectStar()
    {
        assertUpdate("CREATE VIEW test_select_star_view AS SELECT * FROM orders");

        // Oracle expands SELECT * to explicit columns
        assertQuery(
                "SELECT orderkey, custkey, orderstatus FROM test_select_star_view WHERE orderkey < 100",
                "SELECT orderkey, custkey, orderstatus FROM orders WHERE orderkey < 100");

        assertUpdate("DROP VIEW test_select_star_view");
    }

    @Test
    public void testViewWithExpressions()
    {
        @Language("SQL") String viewQuery = "SELECT orderkey, orderstatus, totalprice * 2 AS double_price, " +
                "CASE WHEN orderstatus = 'O' THEN 'Open' ELSE 'Other' END AS status_desc FROM orders";

        assertUpdate("CREATE VIEW test_expression_view AS " + viewQuery);

        assertQuery(
                "SELECT * FROM test_expression_view WHERE orderkey < 100",
                viewQuery + " WHERE orderkey < 100");

        assertUpdate("DROP VIEW test_expression_view");
    }

    @Test
    public void testViewWithAggregation()
    {
        @Language("SQL") String viewQuery = "SELECT orderstatus, COUNT(orderkey) AS order_count, " +
                "SUM(totalprice) AS total_price FROM orders GROUP BY orderstatus";

        assertUpdate("CREATE VIEW test_aggregation_view AS " + viewQuery);

        assertQuery("SELECT * FROM test_aggregation_view", viewQuery);

        assertUpdate("DROP VIEW test_aggregation_view");
    }

    @Test
    public void testViewWithJoin()
    {
        assertUpdate("CREATE VIEW orders_summary AS SELECT orderkey, orderstatus, totalprice FROM orders WHERE orderkey < 1000");

        @Language("SQL") String viewQuery = "SELECT a.orderkey, a.orderstatus, a.totalprice " +
                "FROM orders a JOIN orders_summary b ON a.orderkey = b.orderkey WHERE a.orderkey < 1000";

        assertUpdate("CREATE VIEW test_join_view AS " + viewQuery);

        assertQuery(
                "SELECT orderkey, orderstatus, totalprice FROM test_join_view WHERE orderkey < 100",
                "SELECT a.orderkey, a.orderstatus, a.totalprice FROM orders a " +
                "JOIN (SELECT orderkey, orderstatus, totalprice FROM orders WHERE orderkey < 1000) b " +
                "ON a.orderkey = b.orderkey WHERE a.orderkey < 100");

        assertUpdate("DROP VIEW test_join_view");
        assertUpdate("DROP VIEW orders_summary");
    }

    @Test
    public void testViewWithWhereClause()
    {
        assertUpdate("CREATE VIEW test_where_view AS SELECT * FROM orders WHERE orderstatus = 'F'");

        assertQuery(
                "SELECT orderkey, orderstatus FROM test_where_view WHERE orderkey < 100",
                "SELECT orderkey, orderstatus FROM orders WHERE orderstatus = 'F' AND orderkey < 100");

        assertUpdate("DROP VIEW test_where_view");
    }

    @Test
    public void testViewWithOrderBy()
    {
        assertUpdate("CREATE VIEW test_orderby_view AS SELECT orderkey, orderstatus, totalprice FROM orders");

        // Query the view with ORDER BY
        assertQuery(
                "SELECT * FROM test_orderby_view WHERE orderkey < 100 ORDER BY totalprice DESC",
                "SELECT orderkey, orderstatus, totalprice FROM orders WHERE orderkey < 100 ORDER BY totalprice DESC");

        assertUpdate("DROP VIEW test_orderby_view");
    }

    @Test
    public void testShowCreateView()
    {
        @Language("SQL") String viewQuery = "SELECT orderkey, orderstatus, totalprice FROM orders";
        assertUpdate("CREATE VIEW test_show_create AS " + viewQuery);

        MaterializedResult result = computeActual("SHOW CREATE VIEW test_show_create");
        String createViewSql = (String) result.getOnlyValue();

        // Verify the CREATE VIEW statement contains key elements
        assertTrue(createViewSql.contains("CREATE VIEW"));
        assertTrue(createViewSql.contains("test_show_create"));
        assertTrue(createViewSql.contains("SECURITY DEFINER"));

        assertUpdate("DROP VIEW test_show_create");
    }

    @Test
    public void testListViews()
    {
        // Create multiple views
        assertUpdate("CREATE VIEW test_list_view1 AS SELECT orderkey FROM orders");
        assertUpdate("CREATE VIEW test_list_view2 AS SELECT orderstatus FROM orders");

        // List views in the schema
        MaterializedResult views = computeActual("SHOW TABLES");

        boolean foundView1 = false;
        boolean foundView2 = false;

        for (MaterializedRow row : views.getMaterializedRows()) {
            String tableName = (String) row.getField(0);
            if ("test_list_view1".equalsIgnoreCase(tableName)) {
                foundView1 = true;
            }
            if ("test_list_view2".equalsIgnoreCase(tableName)) {
                foundView2 = true;
            }
        }

        assertTrue(foundView1, "test_list_view1 should be listed");
        assertTrue(foundView2, "test_list_view2 should be listed");

        assertUpdate("DROP VIEW test_list_view1");
        assertUpdate("DROP VIEW test_list_view2");
    }

    @Test
    public void testViewWithCast()
    {
        @Language("SQL") String viewQuery = "SELECT orderkey, CAST(totalprice AS VARCHAR(50)) AS price_str FROM orders";

        assertUpdate("CREATE VIEW test_cast_view AS " + viewQuery);

        assertQuery(
                "SELECT * FROM test_cast_view WHERE orderkey < 100",
                viewQuery + " WHERE orderkey < 100");

        assertUpdate("DROP VIEW test_cast_view");
    }

    @Test
    public void testViewWithSubquery()
    {
        @Language("SQL") String viewQuery = "SELECT orderkey, orderstatus FROM orders " +
                "WHERE orderkey IN (SELECT orderkey FROM orders WHERE totalprice > 100000)";

        assertUpdate("CREATE VIEW test_subquery_view AS " + viewQuery);

        assertQuery("SELECT * FROM test_subquery_view WHERE orderkey < 1000", viewQuery + " AND orderkey < 1000");

        assertUpdate("DROP VIEW test_subquery_view");
    }

    @Test
    public void testViewWithDistinct()
    {
        @Language("SQL") String viewQuery = "SELECT DISTINCT orderstatus FROM orders";

        assertUpdate("CREATE VIEW test_distinct_view AS " + viewQuery);

        assertQuery("SELECT * FROM test_distinct_view", viewQuery);

        assertUpdate("DROP VIEW test_distinct_view");
    }

    @Test
    public void testViewWithLimit()
    {
        assertUpdate("CREATE VIEW test_limit_view AS SELECT orderkey, orderstatus FROM orders");

        // Apply LIMIT when querying the view
        MaterializedResult result = computeActual("SELECT * FROM test_limit_view LIMIT 10");
        assertEquals(result.getRowCount(), 10);

        assertUpdate("DROP VIEW test_limit_view");
    }

    @Test
    public void testViewWithNullValues()
    {
        assertUpdate("CREATE TABLE test_null_table AS SELECT orderkey, " +
                "CASE WHEN MOD(orderkey, 2) = 0 THEN orderstatus ELSE CAST(NULL AS VARCHAR(1)) END AS status " +
                "FROM orders WHERE orderkey < 100",
                "SELECT COUNT(*) FROM orders WHERE orderkey < 100");

        assertUpdate("CREATE VIEW test_null_view AS SELECT orderkey, status FROM test_null_table WHERE status IS NULL");

        MaterializedResult result = computeActual("SELECT COUNT(*) FROM test_null_view");
        assertTrue(result.getRowCount() > 0, "View should return rows with NULL status");

        assertUpdate("DROP VIEW test_null_view");
        assertUpdate("DROP TABLE test_null_table");
    }

    @Test
    public void testViewQualifiedName()
    {
        assertUpdate("CREATE VIEW test_qualified_view AS SELECT orderkey FROM orders");

        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String qualifiedName = format("%s.%s.test_qualified_view", catalog, schema);

        assertQuery(
                "SELECT * FROM " + qualifiedName + " WHERE orderkey < 100",
                "SELECT orderkey FROM orders WHERE orderkey < 100");

        assertUpdate("DROP VIEW test_qualified_view");
    }

    @Test
    public void testDropNonExistentView()
    {
        // Attempting to drop a non-existent view should fail
        assertQueryFails("DROP VIEW non_existent_view", ".*does not exist.*");
    }

    @Test
    public void testCreateViewWithSameName()
    {
        assertUpdate("CREATE VIEW test_duplicate_view AS SELECT orderkey FROM orders");

        // Creating a view with the same name should fail without OR REPLACE
        // Oracle error: ORA-00955: name is already used by an existing object
        assertQueryFails(
                "CREATE VIEW test_duplicate_view AS SELECT orderstatus FROM orders",
                ".*ORA-00955.*");

        assertUpdate("DROP VIEW test_duplicate_view");
    }
}
