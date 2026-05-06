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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for SQL Server view support.
 * Tests basic view operations: CREATE, SHOW, LIST (via SHOW TABLES), and DROP.
 */
public class TestSqlServerViewSupport
        extends AbstractTestIntegrationSmokeTest
{
    private final MSSQLServerContainer<?> sqlServerContainer;

    public TestSqlServerViewSupport()
    {
        this.sqlServerContainer = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
                .acceptLicense();
        this.sqlServerContainer.start();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        sqlServerContainer.stop();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SqlServerQueryRunner.createSqlServerQueryRunner(
                sqlServerContainer,
                ImmutableMap.of(),
                ImmutableList.of(TpchTable.ORDERS));
    }

    @Override
    protected Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("sqlserver")
                .setSchema("dbo")
                .build();
    }

    @Test
    public void testCreateView()
    {
        // Create a simple view
        assertUpdate("CREATE VIEW test_view AS SELECT orderkey, custkey FROM orders");

        // Query the view
        MaterializedResult result = computeActual("SELECT orderkey FROM test_view WHERE orderkey = 1");
        assertEquals(result.getRowCount(), 1);

        // Cleanup
        assertUpdate("DROP VIEW test_view");
    }

    @Test
    public void testShowCreateView()
    {
        // Create a view
        assertUpdate("CREATE VIEW test_show_view AS SELECT orderkey, totalprice FROM orders");

        // Show create view
        MaterializedResult result = computeActual("SHOW CREATE VIEW test_show_view");
        assertEquals(result.getRowCount(), 1);
        String viewDefinition = (String) result.getOnlyValue();
        assertTrue(viewDefinition.contains("CREATE VIEW"), "View definition should contain CREATE VIEW");
        assertTrue(viewDefinition.contains("orderkey"), "View definition should contain orderkey column");
        assertTrue(viewDefinition.contains("totalprice"), "View definition should contain totalprice column");

        // Cleanup
        assertUpdate("DROP VIEW test_show_view");
    }

    @Test
    public void testListViewsWithShowTables()
    {
        // Create test views
        assertUpdate("CREATE VIEW view1 AS SELECT orderkey FROM orders");
        assertUpdate("CREATE VIEW view2 AS SELECT custkey FROM orders");

        // List views using SHOW TABLES (views should appear as tables)
        MaterializedResult result = computeActual("SHOW TABLES");
        List<Object> tableNames = new ArrayList<>(result.getOnlyColumnAsSet());

        boolean foundView1 = false;
        boolean foundView2 = false;
        for (Object tableName : tableNames) {
            String name = (String) tableName;
            if ("view1".equalsIgnoreCase(name)) {
                foundView1 = true;
            }
            if ("view2".equalsIgnoreCase(name)) {
                foundView2 = true;
            }
        }

        assertTrue(foundView1, "view1 should be listed in SHOW TABLES");
        assertTrue(foundView2, "view2 should be listed in SHOW TABLES");

        // Cleanup
        assertUpdate("DROP VIEW view1");
        assertUpdate("DROP VIEW view2");
    }

    @Test
    public void testDropView()
    {
        // Create view
        assertUpdate("CREATE VIEW test_drop_view AS SELECT orderkey FROM orders");

        // Verify view exists
        MaterializedResult result = computeActual("SHOW TABLES");
        List<Object> tableNames = new ArrayList<>(result.getOnlyColumnAsSet());
        boolean viewExists = tableNames.stream()
                .anyMatch(name -> "test_drop_view".equalsIgnoreCase((String) name));
        assertTrue(viewExists, "View should exist after creation");

        // Drop view
        assertUpdate("DROP VIEW test_drop_view");

        // Verify view no longer exists
        result = computeActual("SHOW TABLES");
        tableNames = new ArrayList<>(result.getOnlyColumnAsSet());
        viewExists = tableNames.stream()
                .anyMatch(name -> "test_drop_view".equalsIgnoreCase((String) name));
        assertFalse(viewExists, "View should not exist after dropping");
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(
                    sqlServerContainer.getJdbcUrl(),
                    sqlServerContainer.getUsername(),
                    sqlServerContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
