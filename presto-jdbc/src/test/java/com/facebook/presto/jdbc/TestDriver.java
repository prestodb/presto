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
package com.facebook.presto.jdbc;

import com.facebook.presto.server.TestingPrestoServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDriver
{
    private TestingPrestoServer server;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
    }

    @AfterMethod
    public void teardown()
    {
        closeQuietly(server);
    }

    @Test
    public void testDriverManager()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT 123 x, 'foo' y")) {
                    ResultSetMetaData metadata = rs.getMetaData();

                    assertEquals(metadata.getColumnCount(), 2);

                    assertEquals(metadata.getColumnLabel(1), "x");
                    assertEquals(metadata.getColumnType(1), Types.BIGINT);

                    assertEquals(metadata.getColumnLabel(2), "y");
                    assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);

                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 123);
                    assertEquals(rs.getLong("x"), 123);
                    assertEquals(rs.getString(2), "foo");
                    assertEquals(rs.getString("y"), "foo");

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testGetCatalogs()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                assertRowCount(rs, 1);
                ResultSetMetaData metadata = rs.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);
                assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
                assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);
            }
        }
    }

    @Test
    public void testGetSchemas()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                assertGetSchemasResult(rs, 2);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, null)) {
                assertGetSchemasResult(rs, 2);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("default", null)) {
                assertGetSchemasResult(rs, 2);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("", null)) {
                // all schemas in presto have a catalog name
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("default", "sys")) {
                assertGetSchemasResult(rs, 1);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "sys")) {
                assertGetSchemasResult(rs, 1);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "s_s")) {
                assertGetSchemasResult(rs, 1);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "%s%")) {
                assertGetSchemasResult(rs, 2);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", null)) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", "sys")) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas(null, "unknown")) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("default", "unknown")) {
                assertGetSchemasResult(rs, 0);
            }

            try (ResultSet rs = connection.getMetaData().getSchemas("unknown", "unknown")) {
                assertGetSchemasResult(rs, 0);
            }
        }
    }

    private void assertGetSchemasResult(ResultSet rs, int expectedRows)
            throws SQLException
    {
        assertRowCount(rs, expectedRows);

        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 2);

        assertEquals(metadata.getColumnLabel(1), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_CATALOG");
        assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);
    }

    @Test
    public void testGetTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", null, null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("", null, null, null)) {
                assertTableMetadata(rs);

                // all tables in presto have a catalog name
                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertEquals(rows.size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "", "", "", "", "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertEquals(rows.size(), 0);
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "tables", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, "information_schema", null, null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "", "", "", "", "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, "tables", null)) {
                assertTableMetadata(rs);

                // todo why does Presto require a schema name in this case
                readRows(rs);
                fail("Expected SQLException");
            } catch (SQLException expected) {
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertTrue(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "", "", "", "", "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "inf%", "tables", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "tab%", null)) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("unknown", "information_schema", "tables", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        // todo why does Presto require that the schema name be lower case
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "unknown", "tables", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "unknown", new String[] {"BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "", "", "", "", "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "tables", new String[] {"unknown"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "",  "",  "",  "",  "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "tables", new String[] {"unknown", "BASE TABLE"})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "",  "",  "",  "",  "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "", "", "", "", "", "")));
            }
        }

        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getTables("default", "information_schema", "tables", new String[] {})) {
                assertTableMetadata(rs);

                Set<List<Object>> rows = ImmutableSet.copyOf(readRows(rs));
                assertTrue(rows.contains(ImmutableList.of("default", "information_schema", "tables", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "information_schema", "schemata", "BASE TABLE", "", "", "", "", "", "")));
                assertFalse(rows.contains(ImmutableList.of("default", "sys", "node", "BASE TABLE", "", "", "", "", "", "")));
            }
        }
    }

    private void assertTableMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 10);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(4), "TABLE_TYPE");
        assertEquals(metadata.getColumnType(4), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(5), "REMARKS");
        assertEquals(metadata.getColumnType(5), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(6), "TYPE_CAT");
        assertEquals(metadata.getColumnType(6), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(7), "TYPE_SCHEM");
        assertEquals(metadata.getColumnType(7), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(8), "TYPE_NAME");
        assertEquals(metadata.getColumnType(8), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(9), "SELF_REFERENCING_COL_NAME");
        assertEquals(metadata.getColumnType(9), Types.LONGNVARCHAR);

        assertEquals(metadata.getColumnLabel(10), "REF_GENERATION");
        assertEquals(metadata.getColumnType(10), Types.LONGNVARCHAR);
    }

    @Test
    public void testGetTableTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                List<List<Object>> data = readRows(tableTypes);
                assertEquals(data.size(), 1);
                assertEquals(data.get(0).get(0), "BASE TABLE");

                ResultSetMetaData metadata = tableTypes.getMetaData();
                assertEquals(metadata.getColumnCount(), 1);

                assertEquals(metadata.getColumnLabel(1), "TABLE_TYPE");
                assertEquals(metadata.getColumnType(1), Types.LONGNVARCHAR);
            }
        }
    }

    @Test
    public void testExecute()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertEquals(rs.getLong("x"), 123);
                assertEquals(rs.getString(2), "foo");
                assertEquals(rs.getString("y"), "foo");
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetUpdateCount()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                assertEquals(statement.getUpdateCount(), -1);
            }
        }
    }

    @Test
    public void testResultSetClose()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet result = statement.getResultSet();
                assertFalse(result.isClosed());
                result.close();
                assertTrue(result.isClosed());
            }
        }
    }

    @Test
    public void testGetResultSet()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                ResultSet result = statement.getResultSet();
                assertNotNull(result);
                assertFalse(result.isClosed());
                statement.getMoreResults();
                assertTrue(result.isClosed());

                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                result = statement.getResultSet();
                assertNotNull(result);
                assertFalse(result.isClosed());

                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                assertFalse(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            }
        }
    }

    @Test(expectedExceptions = SQLFeatureNotSupportedException.class, expectedExceptionsMessageRegExp = "Multiple open results not supported")
    public void testGetMoreResultsException()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                assertTrue(statement.execute("SELECT 123 x, 'foo' y"));
                statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        }
    }

    @Test
    public void testConnectionStringWithCatalogAndSchema()
            throws Exception
    {
        String prefix = format("jdbc:presto://%s", server.getAddress());

        Connection connection;
        connection = DriverManager.getConnection(prefix + "/a/b/", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "b");

        connection = DriverManager.getConnection(prefix + "/a/b", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "b");

        connection = DriverManager.getConnection(prefix + "/a/", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "default");

        connection = DriverManager.getConnection(prefix + "/a", "test", null);
        assertEquals(connection.getCatalog(), "a");
        assertEquals(connection.getSchema(), "default");

        connection = DriverManager.getConnection(prefix + "/", "test", null);
        assertEquals(connection.getCatalog(), "default");
        assertEquals(connection.getSchema(), "default");

        connection = DriverManager.getConnection(prefix + "", "test", null);
        assertEquals(connection.getCatalog(), "default");
        assertEquals(connection.getSchema(), "default");
    }

    @Test
    public void testConnectionWithCatalogAndSchema()
            throws Exception
    {
        try (Connection connection = createConnection("default", "information_schema")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT table_catalog, table_schema " +
                        "FROM tables " +
                        "WHERE table_schema = 'sys' AND table_name = 'node'")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 2);
                    assertEquals(metadata.getColumnLabel(1), "table_catalog");
                    assertEquals(metadata.getColumnLabel(2), "table_schema");
                    assertTrue(rs.next());
                    assertEquals(rs.getString("table_catalog"), "default");
                }
            }
        }
    }

    @Test
    public void testConnectionWithCatalog()
            throws Exception
    {
        try (Connection connection = createConnection("default")) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("" +
                        "SELECT table_catalog, table_schema " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = 'sys' AND table_name = 'node'")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 2);
                    assertEquals(metadata.getColumnLabel(1), "table_catalog");
                    assertEquals(metadata.getColumnLabel(2), "table_schema");
                    assertTrue(rs.next());
                    assertEquals(rs.getString("table_catalog"), "default");
                }
            }
        }
    }

    @Test
    public void testConnectionResourceHandling()
            throws Exception
    {
        List<Connection> connections = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            Connection connection = createConnection();
            connections.add(connection);

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT 123")) {
                assertTrue(rs.next());
            }
        }

        for (Connection connection : connections) {
            connection.close();
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".* does not exist")
    public void testBadQuery()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet ignored = statement.executeQuery("SELECT * FROM bad_table")) {
                    fail("expected exception");
                }
            }
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Username property \\(user\\) must be set")
    public void testUserIsRequired()
            throws Exception
    {
        try (Connection ignored = DriverManager.getConnection("jdbc:presto://test.invalid/")) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Invalid path segments in URL: .*")
    public void testBadUrlExtraPathSegments()
            throws Exception
    {
        String url = format("jdbc:presto://%s/hive/default/bad_string", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlMissingCatalog()
            throws Exception
    {
        String url = format("jdbc:presto://%s//default", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Catalog name is empty: .*")
    public void testBadUrlEndsInSlashes()
            throws Exception
    {
        String url = format("jdbc:presto://%s//", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Schema name is empty: .*")
    public void testBadUrlMissingSchema()
            throws Exception
    {
        String url = format("jdbc:presto://%s/a//", server.getAddress());
        try (Connection ignored = DriverManager.getConnection(url, "test", null)) {
            fail("expected exception");
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s", server.getAddress(), catalog);
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    private static void assertRowCount(ResultSet rs, int expected)
            throws SQLException
    {
        List<List<Object>> data = readRows(rs);
        assertEquals(data.size(), expected);
    }

    private static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            ImmutableList.Builder<Object> row = ImmutableList.builder();
            for (int i = 0; i < columnCount; i++) {
                row.add(rs.getObject(i + 1));
            }
            rows.add(row.build());
        }
        return rows.build();
    }

    static void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception ignored) {
        }
    }
}
