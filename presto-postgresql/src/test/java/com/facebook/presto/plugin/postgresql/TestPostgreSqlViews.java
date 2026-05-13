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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for PostgreSQL view operations.
 * Tests CREATE VIEW, DROP VIEW, and listing views functionality.
 */
@Test(singleThreaded = true)
public class TestPostgreSqlViews
{
    private PostgreSQLContainer postgresContainer;
    private QueryRunner queryRunner;
    private Session session;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        postgresContainer = new PostgreSQLContainer("postgres:14")
                .withDatabaseName("tpch")
                .withUsername("testuser")
                .withPassword("testpass");
        postgresContainer.start();

        queryRunner = PostgreSqlQueryRunner.createPostgreSqlQueryRunner(
                postgresContainer.getJdbcUrl(),
                ImmutableMap.of(),
                ImmutableList.of(ORDERS));
        session = PostgreSqlQueryRunner.createSession();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        if (postgresContainer != null) {
            postgresContainer.stop();
            postgresContainer = null;
        }
    }

    @AfterMethod
    public void cleanupViews()
            throws SQLException
    {
        // Clean up any views created during tests
        try (Connection connection = DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
                Statement statement = connection.createStatement()) {
            // Get all views in tpch schema - collect names first
            List<String> viewNames = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'tpch'")) {
                while (rs.next()) {
                    viewNames.add(rs.getString(1));
                }
            }
            // Now drop all views
            for (String viewName : viewNames) {
                statement.execute(format("DROP VIEW IF EXISTS tpch.%s CASCADE", viewName));
            }
        }
    }

    @Test
    public void testCreateView()
            throws Exception
    {
        String viewName = "test_create_view";

        // Create view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey, custkey FROM tpch.orders WHERE orderkey < 100", viewName));

        // Verify view exists in Presto
        assertTrue(queryRunner.tableExists(session, viewName));

        // Verify view can be queried through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT * FROM %s ORDER BY orderkey LIMIT 5", viewName));
        assertTrue(result.getRowCount() > 0);
        assertEquals(result.getTypes().size(), 2);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testCreateOrReplaceView()
            throws Exception
    {
        String viewName = "test_replace_view";

        // Create initial view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey FROM tpch.orders", viewName));

        // Verify initial view through Presto
        MaterializedResult result1 = queryRunner.execute(session, format("SELECT * FROM %s LIMIT 1", viewName));
        assertEquals(result1.getTypes().size(), 1);

        // Replace view with different definition
        execute(format("CREATE OR REPLACE VIEW tpch.%s AS SELECT orderkey, custkey, orderstatus FROM tpch.orders", viewName));

        // Verify replaced view has new structure
        MaterializedResult result2 = queryRunner.execute(session, format("SELECT * FROM %s LIMIT 1", viewName));
        assertEquals(result2.getTypes().size(), 3);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testDropView()
            throws Exception
    {
        String viewName = "test_drop_view";

        // Create view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT * FROM tpch.orders", viewName));
        assertTrue(queryRunner.tableExists(session, viewName));

        // Drop view through PostgreSQL
        execute(format("DROP VIEW tpch.%s", viewName));
        assertFalse(queryRunner.tableExists(session, viewName));
    }

    @Test
    public void testListViews()
            throws Exception
    {
        String view1 = "test_list_view_1";
        String view2 = "test_list_view_2";

        // Create multiple views directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey FROM tpch.orders", view1));
        execute(format("CREATE VIEW tpch.%s AS SELECT custkey FROM tpch.orders", view2));

        // List views using SHOW TABLES through Presto
        MaterializedResult result = queryRunner.execute(session, "SHOW TABLES");
        assertThat(result.getOnlyColumnAsSet()).contains(view1, view2, "orders");

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", view1));
        execute(format("DROP VIEW IF EXISTS tpch.%s", view2));
    }

    @Test
    public void testViewWithJoin()
            throws Exception
    {
        String viewName = "test_view_join";

        // Create a view with self-join directly in PostgreSQL
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT o1.orderkey, o1.custkey, o2.orderstatus " +
                "FROM tpch.orders o1 JOIN tpch.orders o2 ON o1.orderkey = o2.orderkey " +
                "WHERE o1.orderkey < 10", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT * FROM %s", viewName));
        assertTrue(result.getRowCount() > 0);
        assertEquals(result.getTypes().size(), 3);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewWithAggregation()
            throws Exception
    {
        String viewName = "test_view_aggregation";

        // Create view with aggregation directly in PostgreSQL
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT custkey, COUNT(*) as order_count, SUM(totalprice) as total_spent " +
                "FROM tpch.orders " +
                "GROUP BY custkey", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT * FROM %s WHERE custkey = 1", viewName));
        assertEquals(result.getTypes().size(), 3);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewWithFilter()
            throws Exception
    {
        String viewName = "test_view_filter";

        // Create view with WHERE clause directly in PostgreSQL
        // Use column reference instead of string literal to avoid parsing issues
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate " +
                "FROM tpch.orders WHERE orderkey < 100", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT orderkey FROM %s ORDER BY orderkey LIMIT 5", viewName));

        // Verify we got results and they are less than 100
        assertTrue(result.getRowCount() > 0);
        for (int i = 0; i < result.getRowCount(); i++) {
            long orderkey = (long) result.getMaterializedRows().get(i).getField(0);
            assertTrue(orderkey < 100);
        }

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewInInformationSchema()
            throws Exception
    {
        String viewName = "test_info_schema_view";

        // Create view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey, custkey FROM tpch.orders", viewName));

        // Check view appears in information_schema.tables through Presto
        MaterializedResult result = queryRunner.execute(session,
                format("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch' AND table_name = '%s'", viewName));
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getOnlyValue(), viewName);

        // Check view columns appear in information_schema.columns
        MaterializedResult columnsResult = queryRunner.execute(session,
                format("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s' ORDER BY ordinal_position", viewName));
        assertEquals(columnsResult.getRowCount(), 2);
        assertThat(columnsResult.getOnlyColumnAsSet()).contains("orderkey", "custkey");

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewWithComplexTypes()
            throws Exception
    {
        String viewName = "test_view_complex_types";

        // Create view with various column types directly in PostgreSQL
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority " +
                "FROM tpch.orders " +
                "WHERE orderkey < 100", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT * FROM %s LIMIT 5", viewName));
        assertEquals(result.getTypes().size(), 6);
        assertTrue(result.getRowCount() > 0);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewWithSubquery()
            throws Exception
    {
        String viewName = "test_view_subquery";

        // Create view with subquery directly in PostgreSQL
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT * FROM tpch.orders WHERE custkey IN (SELECT custkey FROM tpch.orders WHERE orderkey < 10)", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", viewName));
        assertTrue(result.getRowCount() > 0);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testMultipleViewsInSameSchema()
            throws Exception
    {
        String view1 = "test_multi_view_1";
        String view2 = "test_multi_view_2";
        String view3 = "test_multi_view_3";

        // Create multiple views directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey FROM tpch.orders WHERE orderkey < 100", view1));
        execute(format("CREATE VIEW tpch.%s AS SELECT custkey FROM tpch.orders WHERE custkey < 50", view2));
        execute(format("CREATE VIEW tpch.%s AS SELECT orderstatus FROM tpch.orders", view3));

        // Verify all views exist through Presto
        assertTrue(queryRunner.tableExists(session, view1));
        assertTrue(queryRunner.tableExists(session, view2));
        assertTrue(queryRunner.tableExists(session, view3));

        // Query each view through Presto
        MaterializedResult result1 = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", view1));
        MaterializedResult result2 = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", view2));
        MaterializedResult result3 = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", view3));

        assertTrue(result1.getRowCount() > 0);
        assertTrue(result2.getRowCount() > 0);
        assertTrue(result3.getRowCount() > 0);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", view1));
        execute(format("DROP VIEW IF EXISTS tpch.%s", view2));
        execute(format("DROP VIEW IF EXISTS tpch.%s", view3));
    }

    @Test
    public void testViewNameCaseSensitivity()
            throws Exception
    {
        String viewName = "test_case_view";

        // Create view with lowercase name directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT * FROM tpch.orders", viewName));

        // Query with different case variations through Presto (PostgreSQL is case-insensitive by default)
        MaterializedResult result1 = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", viewName));
        MaterializedResult result2 = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", viewName.toUpperCase()));

        assertEquals(result1.getOnlyValue(), result2.getOnlyValue());

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testDescribeView()
            throws Exception
    {
        String viewName = "test_describe_view";

        // Create view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey, custkey, orderstatus FROM tpch.orders", viewName));

        // Describe view through Presto
        MaterializedResult result = queryRunner.execute(session, format("DESCRIBE %s", viewName));
        assertEquals(result.getRowCount(), 3);

        // Verify column names
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("orderkey");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("custkey");
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("orderstatus");

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testShowCreateView()
            throws Exception
    {
        String viewName = "test_show_create_view";

        // Create view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey, custkey FROM tpch.orders WHERE orderkey < 100", viewName));

        // Show create view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SHOW CREATE VIEW %s", viewName));
        assertEquals(result.getRowCount(), 1);

        String createViewSql = (String) result.getOnlyValue();
        assertThat(createViewSql).contains("CREATE VIEW");
        assertThat(createViewSql).contains(viewName);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewWithOrderBy()
            throws Exception
    {
        String viewName = "test_view_order_by";

        // Create view with ORDER BY directly in PostgreSQL
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT orderkey, custkey, totalprice FROM tpch.orders ORDER BY totalprice DESC", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT * FROM %s LIMIT 10", viewName));
        assertEquals(result.getRowCount(), 10);
        assertEquals(result.getTypes().size(), 3);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewWithLimit()
            throws Exception
    {
        String viewName = "test_view_limit";

        // Create view with LIMIT directly in PostgreSQL
        execute(format(
                "CREATE VIEW tpch.%s AS " +
                "SELECT * FROM tpch.orders LIMIT 50", viewName));

        // Query the view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", viewName));
        assertEquals(result.getOnlyValue(), 50L);

        // Clean up
        execute(format("DROP VIEW IF EXISTS tpch.%s", viewName));
    }

    @Test
    public void testViewDependingOnAnotherView()
            throws Exception
    {
        String baseView = "test_base_view";
        String dependentView = "test_dependent_view";

        // Create base view directly in PostgreSQL
        execute(format("CREATE VIEW tpch.%s AS SELECT orderkey, custkey FROM tpch.orders WHERE orderkey < 100", baseView));

        // Create view depending on base view
        execute(format("CREATE VIEW tpch.%s AS SELECT * FROM tpch.%s WHERE custkey < 50", dependentView, baseView));

        // Query dependent view through Presto
        MaterializedResult result = queryRunner.execute(session, format("SELECT COUNT(*) FROM %s", dependentView));
        assertTrue(result.getRowCount() > 0);

        // Clean up (order matters due to dependency)
        execute(format("DROP VIEW IF EXISTS tpch.%s", dependentView));
        execute(format("DROP VIEW IF EXISTS tpch.%s", baseView));
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
