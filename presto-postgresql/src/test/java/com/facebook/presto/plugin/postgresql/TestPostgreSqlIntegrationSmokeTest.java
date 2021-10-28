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

import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlIntegrationSmokeTest()
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer("testuser", "tpch");
        execute("CREATE EXTENSION file_fdw");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ORDERS);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        postgreSqlServer.close();
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
    }

    @Test
    public void testForeignTable()
            throws Exception
    {
        execute("CREATE SERVER devnull FOREIGN DATA WRAPPER file_fdw");
        execute("CREATE FOREIGN TABLE tpch.test_ft (x bigint) SERVER devnull OPTIONS (filename '/dev/null')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_ft"));
        computeActual("SELECT * FROM test_ft");
        execute("DROP FOREIGN TABLE tpch.test_ft");
        execute("DROP SERVER devnull");
    }

    @Test
    public void testTableWithNoSupportedColumns()
            throws Exception
    {
        String unsupportedDataType = "interval";
        String supportedDataType = "varchar(5)";

        try (AutoCloseable ignore1 = withTable("tpch.no_supported_columns", format("(c %s)", unsupportedDataType));
                AutoCloseable ignore2 = withTable("tpch.supported_columns", format("(good %s)", supportedDataType));
                AutoCloseable ignore3 = withTable("tpch.no_columns", "()")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            assertQueryFails("SELECT c FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT * FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");

            assertQueryFails("SELECT c FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT * FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_columns", "Table 'tpch.no_columns' not found");

            assertQueryFails("SELECT c FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT * FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT 'a' FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");

            assertQuery("SHOW COLUMNS FROM no_supported_columns", "SELECT 'nothing' WHERE false");
            assertQuery("SHOW COLUMNS FROM no_columns", "SELECT 'nothing' WHERE false");

            // Other tables should be visible in SHOW TABLES (the no_supported_columns might be included or might be not) and information_schema.tables
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            // Other tables should be introspectable with SHOW COLUMNS and information_schema.columns
            assertQuery("SHOW COLUMNS FROM supported_columns", "VALUES ('good', 'varchar(5)', '', '')");

            // Listing columns in all tables should not fail due to tables with no columns
            computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch'");
        }
    }

    @Test
    public void testInsertWithFailureDoesntLeaveBehindOrphanedTable()
            throws Exception
    {
        String schemaName = format("tmp_schema_%s", UUID.randomUUID().toString().replaceAll("-", ""));
        try (AutoCloseable schema = withSchema(schemaName);
                AutoCloseable table = withTable(format("%s.test_cleanup", schemaName), "(x INTEGER)")) {
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES 'test_cleanup'");

            execute(format("ALTER TABLE %s.test_cleanup ADD CHECK (x > 0)", schemaName));

            assertQueryFails(format("INSERT INTO %s.test_cleanup (x) VALUES (0)", schemaName), "ERROR: new row .* violates check constraint [\\s\\S]*");
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES 'test_cleanup'");
        }
    }

    @Test
    public void testCharTrailingSpace()
            throws Exception
    {
        execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
        assertUpdate("INSERT INTO char_trailing_space VALUES ('test')", 1);

        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test'", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test  '", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test        '", "VALUES 'test'");

        assertEquals(getQueryRunner().execute("SELECT * FROM char_trailing_space WHERE x = char ' test'").getRowCount(), 0);

        Map<String, String> properties = ImmutableMap.of("deprecated.legacy-char-to-varchar-coercion", "true");
        Map<String, String> connectorProperties = ImmutableMap.of("connection-url", postgreSqlServer.getJdbcUrl());

        try (QueryRunner queryRunner = new DistributedQueryRunner(getSession(), 3, properties);) {
            queryRunner.installPlugin(new PostgreSqlPlugin());
            queryRunner.createCatalog("postgresql", "postgresql", connectorProperties);

            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test'").getRowCount(), 0);
            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test  '").getRowCount(), 0);
            assertEquals(queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test       '").getRowCount(), 0);

            MaterializedResult result = queryRunner.execute("SELECT * FROM char_trailing_space WHERE x = char 'test      '");
            assertEquals(result.getRowCount(), 1);
            assertEquals(result.getMaterializedRows().get(0).getField(0), "test      ");
        }

        assertUpdate("DROP TABLE char_trailing_space");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        String createTableFormat = "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                "   %s date,\n" +
                "   %s date NOT NULL\n" +
                ")";
        @Language("SQL") String createTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "column_a",
                "column_b");
        @Language("SQL") String expectedShowCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "\"column_a\"",
                "\"column_b\"");
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), expectedShowCreateTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_b");
        assertQueryFails("INSERT INTO test_insert_not_null (column_a, column_b) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_b");

        assertUpdate("ALTER TABLE test_insert_not_null ADD COLUMN column_c BIGINT NOT NULL");

        createTableFormat = "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                "   %s date,\n" +
                "   %s date NOT NULL,\n" +
                "   %s bigint NOT NULL\n" +
                ")";
        expectedShowCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                "\"column_a\"",
                "\"column_b\"",
                "\"column_c\"");
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), expectedShowCreateTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (date '2012-12-31')", "NULL value not allowed for NOT NULL column: column_c");
        assertQueryFails("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', null)", "NULL value not allowed for NOT NULL column: column_c");

        assertUpdate("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_insert_not_null (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_insert_not_null",
                "VALUES (NULL, CAST('2012-12-31' AS DATE), 1), (CAST('2013-01-01' AS DATE), CAST('2013-01-02' AS DATE), 2)");

        assertUpdate("DROP TABLE test_insert_not_null");
    }

    @Test
    public void testColumnComment()
            throws Exception
    {
        execute("create table tpch.test_column_comment (column_a char(3), column_b int, column_c int)");
        execute("comment on column tpch.test_column_comment.column_a is 'first field'");
        execute("comment on column tpch.test_column_comment.column_b is ''");
        assertQuery(
                "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = 'test_column_comment'",
                "VALUES ('column_a', 'first field'), ('column_b', null), ('column_c', null)");

        assertUpdate("DROP TABLE test_column_comment");
    }

    private AutoCloseable withSchema(String schema)
            throws Exception
    {
        execute(format("CREATE SCHEMA %s", schema));
        return () -> {
            try {
                execute(format("DROP SCHEMA %s", schema));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
            throws Exception
    {
        execute(format("CREATE TABLE %s%s", tableName, tableDefinition));
        return () -> {
            try {
                execute(format("DROP TABLE %s", tableName));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void execute(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
