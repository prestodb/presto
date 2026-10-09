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
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
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
import java.util.Optional;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.MYSQL_CATALOG;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMySqlViews
        extends AbstractTestQueryFramework
{
    private final MySQLContainer mysqlContainer;

    public TestMySqlViews()
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
                    // SET_USER_ID lets the connection user create views with the Presto user as DEFINER
                    "-e", "CREATE DATABASE IF NOT EXISTS test_database; GRANT ALL PRIVILEGES ON test_database.* TO 'testuser'@'%'; " +
                            "GRANT SET_USER_ID ON *.* TO 'testuser'@'%';");
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

        try {
            assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
            assertTrue(viewExistsInMySQL(viewName), "View should exist in MySQL");
            assertQuery("SELECT orderkey FROM " + viewName + " LIMIT 1", "VALUES 1");
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    @Test
    public void testCreateOrReplaceView()
            throws SQLException
    {
        String viewName = "test_replace_view";
        String viewDefinition1 = "SELECT orderkey FROM tpch.orders WHERE orderkey < 50";
        String viewDefinition2 = "SELECT orderkey, custkey FROM tpch.orders WHERE orderkey < 100";
        dropViewIfExists(viewName);

        try {
            assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition1);
            assertTrue(viewExistsInMySQL(viewName), "View should exist after creation");

            assertUpdate("CREATE OR REPLACE VIEW " + viewName + " AS " + viewDefinition2);
            assertTrue(viewExistsInMySQL(viewName), "View should still exist after replacement");

            assertQuerySucceeds("SELECT * FROM " + viewName);
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    @Test
    public void testCreateViewAlreadyExists()
            throws SQLException
    {
        String viewName = "test_create_view_already_exists";
        String viewDefinition = "SELECT orderkey FROM tpch.orders WHERE orderkey < 100";
        dropViewIfExists(viewName);

        try {
            assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
            assertTrue(viewExistsInMySQL(viewName), "View should exist after creation");

            assertQueryFails(
                    "CREATE VIEW " + viewName + " AS " + viewDefinition,
                    ".*already exists.*");
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    @Test
    public void testDropView()
            throws SQLException
    {
        String viewName = "test_drop_view";
        String viewDefinition = "SELECT orderkey FROM tpch.orders";
        dropViewIfExists(viewName);

        try {
            assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
            assertTrue(viewExistsInMySQL(viewName), "View should exist after creation");

            assertUpdate("DROP VIEW " + viewName);
            assertFalse(viewExistsInMySQL(viewName), "View should not exist after drop");
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    @Test
    public void testRenameView()
            throws SQLException
    {
        String oldViewName = "test_rename_view_old";
        String newViewName = "test_rename_view_new";
        String viewDefinition = "SELECT orderkey, custkey FROM tpch.orders";
        dropViewIfExists(oldViewName, newViewName);

        try {
            assertUpdate("CREATE VIEW " + oldViewName + " AS " + viewDefinition);
            assertTrue(viewExistsInMySQL(oldViewName), "Old view should exist after creation");

            assertUpdate("ALTER VIEW " + oldViewName + " RENAME TO " + newViewName);
            assertFalse(viewExistsInMySQL(oldViewName), "Old view should not exist after rename");
            assertTrue(viewExistsInMySQL(newViewName), "New view should exist after rename");

            assertQuerySucceeds("SELECT * FROM " + newViewName);
        }
        finally {
            dropViewIfExists(oldViewName, newViewName);
        }
    }

    @Test
    public void testListViews()
            throws SQLException
    {
        String view1 = "test_list_view_1";
        String view2 = "test_list_view_2";
        String viewDefinition = "SELECT orderkey FROM tpch.orders";
        dropViewIfExists(view1, view2);

        try {
            assertUpdate("CREATE VIEW " + view1 + " AS " + viewDefinition);
            assertUpdate("CREATE VIEW " + view2 + " AS " + viewDefinition);

            List<Object> tables = getQueryRunner().execute(getSession(), "SHOW TABLES")
                    .getOnlyColumn()
                    .collect(toImmutableList());

            assertTrue(tables.contains(view1), "View 1 should be in the list");
            assertTrue(tables.contains(view2), "View 2 should be in the list");
        }
        finally {
            dropViewIfExists(view1, view2);
        }
    }

    @Test
    public void testGetViews()
            throws SQLException
    {
        String viewName = "test_get_views";
        String viewDefinition = "SELECT orderkey, custkey, orderstatus FROM tpch.orders WHERE orderkey < 100";
        dropViewIfExists(viewName);

        try {
            assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);

            // information_schema.views is served by Metadata.getViews, so reading it through Presto
            // exercises JdbcMetadata.getViews -> MySqlClient.getViews and the ConnectorViewDefinition
            // it builds, rather than only checking that MySQL recorded the view.
            MaterializedResult views = computeActual(
                    "SELECT table_catalog, table_schema, table_name, view_owner, view_definition " +
                            "FROM information_schema.views " +
                            "WHERE table_schema = 'tpch' AND table_name = '" + viewName + "'");
            assertEquals(views.getRowCount(), 1);

            MaterializedRow view = views.getMaterializedRows().get(0);
            assertEquals(view.getField(0), MYSQL_CATALOG);
            assertEquals(view.getField(1), "tpch");
            assertEquals(view.getField(2), viewName);
            // the owner is the Presto user who created the view, not the connection user
            assertEquals(view.getField(3), getSession().getUser());

            // MySQL stores its own canonical form of the definition, so assert on what has to survive:
            // the projected columns, and no back ticks, which the Presto analyzer cannot parse.
            String storedSql = (String) view.getField(4);
            assertTrue(storedSql.toLowerCase(ENGLISH).startsWith("select"), "Unexpected view definition: " + storedSql);
            assertFalse(storedSql.contains("`"), "Back ticks should be replaced in: " + storedSql);
            for (String column : new String[] {"orderkey", "custkey", "orderstatus"}) {
                assertTrue(storedSql.contains("\"" + column + "\""), "View definition should project " + column + ", was: " + storedSql);
            }

            // the columns reported by getViews also have to let the analyzer resolve the view
            assertQuery(
                    "SELECT orderkey, custkey, orderstatus FROM " + viewName,
                    "SELECT orderkey, custkey, orderstatus FROM orders WHERE orderkey < 100");
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    @Test
    public void testViewOwnerAndSecurityRoundTrip()
            throws SQLException
    {
        // alice exercises a name MySQL has to quote, carol one that contains the @ separating
        // user from host in the DEFINER that INFORMATION_SCHEMA.VIEWS reports
        String definerView = "test_definer_view";
        String invokerView = "test_invoker_view";
        String emailOwnerView = "test_email_owner_view";
        dropViewIfExists(definerView, invokerView, emailOwnerView);

        try {
            assertUpdate(sessionFor("alice"), "CREATE VIEW " + definerView + " SECURITY DEFINER AS SELECT orderkey FROM tpch.orders");
            assertUpdate(sessionFor("bob"), "CREATE VIEW " + invokerView + " SECURITY INVOKER AS SELECT orderkey FROM tpch.orders");
            assertUpdate(sessionFor("carol@example.com"), "CREATE VIEW " + emailOwnerView + " AS SELECT orderkey FROM tpch.orders");

            // MySQL records the Presto user as DEFINER and the requested security mode
            assertEquals(mySqlViewSecurity(definerView), "alice@% DEFINER");
            assertEquals(mySqlViewSecurity(invokerView), "bob@% INVOKER");
            assertEquals(mySqlViewSecurity(emailOwnerView), "carol@example.com@% DEFINER");

            // read back through Presto, a DEFINER view is owned by its creator, while an INVOKER view
            // has no owner, the same as CreateViewTask records it, so it runs as the querying user
            assertQuery(
                    "SELECT table_name, view_owner FROM information_schema.views " +
                            "WHERE table_schema = 'tpch' AND table_name IN ('" + definerView + "', '" + invokerView + "', '" + emailOwnerView + "')",
                    "VALUES ('" + definerView + "', 'alice'), ('" + invokerView + "', NULL), ('" + emailOwnerView + "', 'carol@example.com')");
            assertTrue(showCreateView(definerView).contains("SECURITY DEFINER"), showCreateView(definerView));
            assertTrue(showCreateView(invokerView).contains("SECURITY INVOKER"), showCreateView(invokerView));

            // replacing a view as another user moves ownership and security mode to the new definition
            assertUpdate(sessionFor("bob"), "CREATE OR REPLACE VIEW " + definerView + " SECURITY INVOKER AS SELECT orderkey FROM tpch.orders");
            assertEquals(mySqlViewSecurity(definerView), "bob@% INVOKER");
            assertTrue(showCreateView(definerView).contains("SECURITY INVOKER"), showCreateView(definerView));
        }
        finally {
            dropViewIfExists(definerView, invokerView, emailOwnerView);
        }
    }

    @Test
    public void testDefinerUser()
    {
        assertEquals(MySqlClient.extractDefinerUser("alice@%"), "alice");
        assertEquals(MySqlClient.extractDefinerUser("root@localhost"), "root");
        assertEquals(MySqlClient.extractDefinerUser("carol@example.com@%"), "carol@example.com");
        assertEquals(MySqlClient.extractDefinerUser("nohost"), "nohost");
    }

    @Test
    public void testGetViewsWithQuoteInViewName()
            throws SQLException
    {
        // MySQL allows a quote inside a back tick quoted identifier, so the name reaches
        // INFORMATION_SCHEMA.VIEWS as a value and has to be bound as a parameter rather than
        // interpolated into the lookup
        String viewName = "test_o'brien_view";
        executeOnMySql("DROP VIEW IF EXISTS tpch.`" + viewName + "`");

        try {
            executeOnMySql("CREATE VIEW tpch.`" + viewName + "` AS SELECT orderkey FROM tpch.orders");

            // the table_name predicate narrows the prefix to a single view, the same path a reference
            // to the view in a query takes
            MaterializedResult singleView = computeActual(
                    "SELECT table_name FROM information_schema.views " +
                            "WHERE table_schema = 'tpch' AND table_name = 'test_o''brien_view'");
            assertEquals(singleView.getRowCount(), 1);
            assertEquals(singleView.getMaterializedRows().get(0).getField(0), viewName);

            // without the predicate the whole schema is listed, which reads the name back out of MySQL
            List<Object> schemaViews = computeActual(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'tpch'")
                    .getOnlyColumn()
                    .collect(toImmutableList());
            assertTrue(schemaViews.contains(viewName), "Quoted view name should be listed, was: " + schemaViews);
        }
        finally {
            executeOnMySql("DROP VIEW IF EXISTS tpch.`" + viewName + "`");
        }
    }

    @Test
    public void testGetViewsListsEveryViewInSchema()
            throws SQLException
    {
        String view1 = "test_get_views_schema_1";
        String view2 = "test_get_views_schema_2";
        String viewDefinition = "SELECT orderkey FROM tpch.orders";
        dropViewIfExists(view1, view2);

        try {
            assertUpdate("CREATE VIEW " + view1 + " AS " + viewDefinition);
            assertUpdate("CREATE VIEW " + view2 + " AS " + viewDefinition);

            // a prefix carrying a schema but no table name is answered by one lookup covering every
            // view in the schema, so both views have to come back keyed by their own name
            List<Object> views = computeActual(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'tpch'")
                    .getOnlyColumn()
                    .collect(toImmutableList());

            assertTrue(views.contains(view1), "View 1 should be listed, was: " + views);
            assertTrue(views.contains(view2), "View 2 should be listed, was: " + views);
        }
        finally {
            dropViewIfExists(view1, view2);
        }
    }

    @Test
    public void testGetViewsWithSameNameInAnotherSchema()
            throws SQLException
    {
        String viewName = "test_get_views_same_name";
        dropViewIfExists(viewName);
        executeOnMySql("DROP VIEW IF EXISTS test_database." + viewName);

        try {
            executeOnMySql("CREATE VIEW tpch." + viewName + " AS SELECT orderkey FROM tpch.orders");
            executeOnMySql("CREATE VIEW test_database." + viewName + " AS SELECT custkey, orderstatus FROM tpch.orders");

            // the columns of the views are read in one metadata call covering the whole prefix, so a
            // view of the same name in another database must neither add columns nor replace them
            assertQuery(
                    "SELECT table_schema, column_name FROM information_schema.columns WHERE table_name = '" + viewName + "'",
                    "VALUES ('tpch', 'orderkey'), ('test_database', 'custkey'), ('test_database', 'orderstatus')");
            assertQuery(
                    "SELECT table_schema, column_name FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '" + viewName + "'",
                    "VALUES ('tpch', 'orderkey')");
            assertQuery(
                    "SELECT table_schema FROM information_schema.views WHERE table_name = '" + viewName + "'",
                    "VALUES ('tpch'), ('test_database')");

            assertQuery("SELECT * FROM tpch." + viewName, "SELECT orderkey FROM orders");
            assertQuery("SELECT * FROM test_database." + viewName, "SELECT custkey, orderstatus FROM orders");
        }
        finally {
            dropViewIfExists(viewName);
            executeOnMySql("DROP VIEW IF EXISTS test_database." + viewName);
        }
    }

    @Test
    public void testCreateViewWithQueryMySqlCannotRun()
            throws SQLException
    {
        String viewName = "test_create_view_not_mysql";
        dropViewIfExists(viewName);

        try {
            // the view query goes to MySQL unchanged, so only a schema qualified name without quotes works
            for (String query : new String[] {
                    "SELECT orderkey FROM " + MYSQL_CATALOG + ".tpch.orders",
                    "SELECT orderkey FROM tpch.\"orders\"",
                    "SELECT orderkey FROM orders"}) {
                assertQueryFails(
                        "CREATE VIEW tpch." + viewName + " AS " + query,
                        "(?s)The query of a view in a MySQL catalog is sent to MySQL unchanged and must be valid MySQL SQL, .*MySQL reported: .*");
                assertFalse(viewExistsInMySQL(viewName), "No view should be created for: " + query);
            }
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    @Test
    public void testViewWithAliasAndOrderBy()
            throws SQLException
    {
        // unlike testCreateView, MySQL rewrites this definition with a table alias and an ORDER BY,
        // and the Presto analyzer has to parse that stored form back when the view is read
        String viewName = "test_complex_view";
        String viewDefinition = "SELECT o.orderkey, o.custkey, o.totalprice, o.orderdate " +
                "FROM tpch.orders o WHERE o.totalprice > 100000 ORDER BY o.totalprice DESC";
        dropViewIfExists(viewName);

        try {
            assertUpdate("CREATE VIEW " + viewName + " AS " + viewDefinition);
            assertTrue(viewExistsInMySQL(viewName), "Complex view should exist");
            assertQuery(
                    "SELECT orderkey, custkey FROM " + viewName,
                    "SELECT orderkey, custkey FROM orders WHERE totalprice > 100000");
        }
        finally {
            dropViewIfExists(viewName);
        }
    }

    private Session sessionFor(String user)
    {
        return testSessionBuilder()
                .setCatalog(MYSQL_CATALOG)
                .setSchema("tpch")
                .setIdentity(new Identity(user, Optional.empty()))
                .build();
    }

    private String showCreateView(String viewName)
    {
        return (String) computeActual("SHOW CREATE VIEW " + viewName).getOnlyValue();
    }

    private String mySqlViewSecurity(String viewName)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(
                        "SELECT DEFINER, SECURITY_TYPE FROM information_schema.views " +
                                "WHERE table_schema = 'tpch' AND table_name = '" + viewName + "'")) {
            assertTrue(rs.next(), "View " + viewName + " should exist in MySQL");
            return rs.getString("DEFINER") + " " + rs.getString("SECURITY_TYPE");
        }
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

    private void dropViewIfExists(String... viewNames)
            throws SQLException
    {
        for (String viewName : viewNames) {
            executeOnMySql("DROP VIEW IF EXISTS tpch." + viewName);
        }
    }

    private void executeOnMySql(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
