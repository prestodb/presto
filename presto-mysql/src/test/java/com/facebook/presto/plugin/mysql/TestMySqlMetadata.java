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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.mysql.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMySqlMetadata
        extends AbstractTestQueryFramework
{
    private final MySQLContainer mysqlContainer;

    public TestMySqlMetadata()
    {
        this.mysqlContainer = new MySQLContainer("mysql:8.0")
                .withDatabaseName("tpch")
                .withUsername("testuser")
                .withPassword("testpass");
        this.mysqlContainer.start();
        try {
            this.mysqlContainer.execInContainer("mysql",
                    "-u", "root",
                    "-p" + mysqlContainer.getPassword(),
                    "-e", "CREATE DATABASE IF NOT EXISTS test_database; GRANT ALL PRIVILEGES ON test_database.* TO 'testuser'@'%';");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to set up test_database", e);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMySqlQueryRunner(mysqlContainer.getJdbcUrl(), ImmutableMap.of(), ImmutableList.of(ORDERS));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        mysqlContainer.stop();
    }

    @Test
    public void testCreateView()
            throws SQLException
    {
        String viewName = "test_create_view";
        String viewDefinition = "SELECT orderkey, custkey FROM tpch.orders WHERE orderkey < 100";

        dropViewIfExists(viewName);

        assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
        assertTrue(viewExistsInMySQL(viewName), "View should exist in MySQL");
        assertQuery("SELECT orderkey FROM " + viewName + " LIMIT 1", "VALUES 1");

        dropViewIfExists(viewName);
    }

    @Test
    public void testCreateOrReplaceView()
            throws SQLException
    {
        String viewName = "test_replace_view";
        String viewDefinition1 = "SELECT orderkey FROM tpch.orders WHERE orderkey < 50";
        String viewDefinition2 = "SELECT orderkey, custkey FROM tpch.orders WHERE orderkey < 100";

        dropViewIfExists(viewName);

        assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition1);
        assertTrue(viewExistsInMySQL(viewName), "View should exist after creation");

        assertUpdate("CREATE OR REPLACE VIEW " + viewName + " AS " + viewDefinition2);
        assertTrue(viewExistsInMySQL(viewName), "View should still exist after replacement");

        assertQuerySucceeds("SELECT * FROM " + viewName);

        dropViewIfExists(viewName);
    }

    @Test
    public void testDropView()
            throws SQLException
    {
        String viewName = "test_drop_view";
        String viewDefinition = "SELECT orderkey FROM tpch.orders";

        dropViewIfExists(viewName);

        assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
        assertTrue(viewExistsInMySQL(viewName), "View should exist after creation");

        assertUpdate("DROP VIEW " + viewName);
        assertFalse(viewExistsInMySQL(viewName), "View should not exist after drop");
    }

    @Test
    public void testRenameView()
            throws SQLException
    {
        String oldViewName = "test_rename_view_old";
        String newViewName = "test_rename_view_new";
        String viewDefinition = "SELECT orderkey, custkey FROM tpch.orders";

        dropViewIfExists(new String[]{oldViewName, newViewName});

        assertUpdate("CREATE VIEW " + oldViewName + " AS " + viewDefinition);
        assertTrue(viewExistsInMySQL(oldViewName), "Old view should exist after creation");

        assertUpdate("ALTER VIEW " + oldViewName + " RENAME TO " + newViewName);
        assertFalse(viewExistsInMySQL(oldViewName), "Old view should not exist after rename");
        assertTrue(viewExistsInMySQL(newViewName), "New view should exist after rename");

        assertQuerySucceeds("SELECT * FROM " + newViewName);

        dropViewIfExists(newViewName);
    }

    @Test
    public void testListViews()
            throws SQLException
    {
        String view1 = "test_list_view_1";
        String view2 = "test_list_view_2";
        String viewDefinition = "SELECT orderkey FROM tpch.orders";

        dropViewIfExists(new String[]{view1, view2});

        assertUpdate("CREATE VIEW " + view1 + " AS " + viewDefinition);
        assertUpdate("CREATE VIEW " + view2 + " AS " + viewDefinition);

        List<Object> tables = getQueryRunner().execute(getSession(), "SHOW TABLES")
                .getOnlyColumn()
                .collect(ImmutableList.toImmutableList());

        assertTrue(tables.contains(view1), "View 1 should be in the list");
        assertTrue(tables.contains(view2), "View 2 should be in the list");

        dropViewIfExists(new String[]{view1, view2});
    }

    @Test
    public void testGetViews()
            throws SQLException
    {
        String viewName = "test_get_views";
        String viewDefinition = "SELECT orderkey, custkey, orderstatus FROM tpch.orders WHERE orderkey < 100";

        dropViewIfExists(viewName);

        assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
        assertTrue(viewExistsInMySQL(viewName), "View should be in results");

        dropViewIfExists(viewName);
    }

    @Test
    public void testViewWithComplexQuery()
            throws SQLException
    {
        String viewName = "test_complex_view";
        String viewDefinition = "SELECT o.orderkey, o.custkey, o.totalprice, o.orderdate " +
                "FROM tpch.orders o WHERE o.totalprice > 100000 ORDER BY o.totalprice DESC";

        dropViewIfExists(viewName);

        assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
        assertTrue(viewExistsInMySQL(viewName), "Complex view should exist");
        assertQuerySucceeds("SELECT * FROM " + viewName + " LIMIT 10");

        dropViewIfExists(viewName);
    }

    @Test
    public void testViewInExplicitSchema()
            throws SQLException
    {
        String viewName = "test_schema_view";
        String viewDefinition = "SELECT orderkey FROM tpch.orders";

        Session tpchSession = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("tpch")
                .build();

        dropViewIfExists(viewName);

        assertUpdate(tpchSession, "CREATE VIEW " + viewName + " AS " + viewDefinition);
        assertTrue(viewExistsInMySQL(viewName), "View should exist in tpch schema");
        assertQuerySucceeds(tpchSession, "SELECT * FROM " + viewName);

        dropViewIfExists(viewName);
    }

    private boolean viewExistsInMySQL(String viewName)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT COUNT(*) FROM information_schema.views " +
                                "WHERE table_schema = 'tpch' AND table_name = '" + viewName + "'")) {
            return rs.next() && rs.getInt(1) > 0;
        }
    }

    private void dropViewIfExists(String[] viewNames)
            throws SQLException
    {
        for (String viewName : viewNames) {
            dropViewIfExists(viewName);
        }
    }

    private void dropViewIfExists(String viewName)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP VIEW IF EXISTS tpch." + viewName);
        }
    }
}
