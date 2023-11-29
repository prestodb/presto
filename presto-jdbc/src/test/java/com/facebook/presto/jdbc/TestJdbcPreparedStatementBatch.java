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

import com.facebook.airlift.log.Logging;
import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestJdbcPreparedStatementBatch
{
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new HiveHadoop2Plugin());
        server.createCatalog("hive", "hive-hadoop2", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", server.getDataDirectory().resolve("hive").toFile().toURI().toString())
                .put("hive.security", "sql-standard")
                .build());

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET ROLE admin");
            statement.executeUpdate("CREATE SCHEMA hive.default");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(server);
    }

    @Test
    public void testBasicExecuteBatchForInsertValues()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_batch (" +
                        "c_boolean boolean, " +
                        "c_bigint bigint, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_null bigint)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_batch VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                statement.setBoolean(1, true);
                statement.setLong(2, 5L);
                statement.setDouble(3, 7.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(8L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);
                statement.addBatch();

                statement.setBoolean(1, false);
                statement.setLong(2, 6L);
                statement.setDouble(3, 8.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(9L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);
                statement.addBatch();

                assertEquals(statement.executeBatch(), new int[] {1, 1});
                // executeBatch() would clear the parameter lines added to batch
                assertEquals(statement.executeBatch(), new int[0]);

                // current parameter line would not be cleared automatically
                assertFalse(statement.execute());
                assertEquals(statement.getUpdateCount(), 1);
                assertEquals(statement.getLargeUpdateCount(), 1);

                statement.clearParameters();
                try {
                    statement.execute();
                    fail("expected exception");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage().contains("Incorrect number of parameters: expected 7 but found 0"));
                }
            }

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT count(*) from test_execute_batch")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 3);
                statement.execute("DROP TABLE test_execute_batch");
            }
        }
    }

    @Test
    public void testInsertMultipleRowInABatchLine()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_batch_multiple_line (" +
                        "c_boolean boolean, " +
                        "c_bigint bigint, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_null bigint)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_batch_multiple_line VALUES (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)")) {
                statement.setBoolean(1, true);
                statement.setLong(2, 5L);
                statement.setDouble(3, 7.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(8L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);
                statement.setBoolean(8, false);
                statement.setLong(9, 6L);
                statement.setDouble(10, 8.0d);
                statement.setBigDecimal(11, BigDecimal.valueOf(9L));
                statement.setString(12, "abc'xyz");
                statement.setBytes(13, "xyz".getBytes(UTF_8));
                statement.setNull(14, Types.BIGINT);
                statement.addBatch();

                statement.setBoolean(1, true);
                statement.setLong(2, 7L);
                statement.setDouble(3, 9.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(10L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);
                statement.setBoolean(8, false);
                statement.setLong(9, 8L);
                statement.setDouble(10, 10.0d);
                statement.setBigDecimal(11, BigDecimal.valueOf(11L));
                statement.setString(12, "abc'xyz");
                statement.setBytes(13, "xyz".getBytes(UTF_8));
                statement.setNull(14, Types.BIGINT);
                statement.addBatch();

                assertEquals(statement.executeBatch(), new int[] {2, 2});
                assertEquals(statement.executeBatch(), new int[0]);

                assertFalse(statement.execute());
                assertEquals(statement.getUpdateCount(), 2);
                assertEquals(statement.getLargeUpdateCount(), 2);

                statement.clearParameters();
                try {
                    statement.execute();
                    fail("expected exception");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage().contains("Incorrect number of parameters: expected 14 but found 0"));
                }
            }

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT count(*) from test_execute_batch_multiple_line")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 6);
                statement.execute("DROP TABLE test_execute_batch_multiple_line");
            }
        }
    }

    @Test
    public void testMustSetAllParametersWhenAddBatch()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_batch_set_parameter (" +
                        "c_boolean boolean, " +
                        "c_bigint bigint, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_null bigint)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_batch_set_parameter VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                statement.setBoolean(1, true);
                statement.setLong(2, 5L);
                statement.setDouble(3, 7.0d);
                statement.setString(5, "abc'xyz");
                statement.setNull(7, Types.BIGINT);
                try {
                    statement.addBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Parameters is not compatible");
                }
                statement.setBigDecimal(4, BigDecimal.valueOf(8L));
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.addBatch();

                // addBatch() do not clear current line of parameters, so we manually clear it
                statement.clearParameters();
                statement.setDouble(3, 8.0d);
                statement.setBigDecimal(4, BigDecimal.valueOf(9L));
                statement.setString(5, "abc'xyz");
                statement.setBytes(6, "xyz".getBytes(UTF_8));
                statement.setNull(7, Types.BIGINT);
                try {
                    statement.addBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Parameters is not compatible");
                }

                statement.setBoolean(1, false);
                statement.setLong(2, 6L);
                statement.addBatch();

                assertEquals(statement.executeBatch(), new int[] {1, 1});
                assertEquals(statement.executeBatch(), new int[0]);

                assertFalse(statement.execute());
                assertEquals(statement.getUpdateCount(), 1);
                assertEquals(statement.getLargeUpdateCount(), 1);

                assertFalse(statement.execute());
                assertEquals(statement.getUpdateCount(), 1);
                assertEquals(statement.getLargeUpdateCount(), 1);
            }

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT count(*) from test_execute_batch_set_parameter")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 4);
                statement.execute("DROP TABLE test_execute_batch_set_parameter");
            }
        }
    }

    @Test
    public void testNoneInsertValuesStatement()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_batch_non_insert_values (" +
                        "c_boolean boolean, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_null bigint, " +
                        "c_bigint bigint) with (partitioned_by = ARRAY['c_bigint'])");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_batch_non_insert_values VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                statement.setBoolean(1, true);
                statement.setDouble(2, 7.0d);
                statement.setBigDecimal(3, BigDecimal.valueOf(8L));
                statement.setString(4, "abc'xyz1");
                statement.setBytes(5, "xyz1".getBytes(UTF_8));
                statement.setNull(6, Types.BIGINT);
                statement.setLong(7, 5L);
                statement.addBatch();

                statement.setBoolean(1, true);
                statement.setDouble(2, 8.0d);
                statement.setBigDecimal(3, BigDecimal.valueOf(9L));
                statement.setString(4, "abc'xyz2");
                statement.setBytes(5, "xyz2".getBytes(UTF_8));
                statement.setNull(6, Types.BIGINT);
                statement.setLong(7, 5L);
                statement.addBatch();

                statement.setBoolean(1, false);
                statement.setDouble(2, 9.0d);
                statement.setBigDecimal(3, BigDecimal.valueOf(10L));
                statement.setString(4, "abc'xyz3");
                statement.setBytes(5, "xyz3".getBytes(UTF_8));
                statement.setNull(6, Types.BIGINT);
                statement.setLong(7, 6L);
                statement.addBatch();

                assertEquals(statement.executeBatch(), new int[]{1, 1, 1});
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT * FROM test_execute_batch_non_insert_values where c_bigint = ?")) {
                statement.setLong(1, 6);
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());
                assertEquals(rs.getDouble(2), 9.0d);
                assertEquals(rs.getString(4), "abc'xyz3");
                assertEquals(rs.getLong(6), 0);
                assertFalse(rs.next());
                rs.close();
                try {
                    statement.addBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Add batch is only supported for insert values with parameter statement");
                }

                try {
                    statement.executeBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Execute batch is only supported for insert values with parameter statement");
                }

                boolean queryFlag = statement.execute();
                assertTrue(queryFlag);
                ResultSet rs2 = statement.getResultSet();
                assertTrue(rs2.next());
                assertEquals(rs2.getDouble(2), 9.0d);
                assertEquals(rs2.getString(4), "abc'xyz3");
                assertEquals(rs2.getLong(6), 0);
                rs2.close();
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "CREATE TABLE test_execute_batch_2 as SELECT * FROM test_execute_batch_non_insert_values where c_bigint = ?")) {
                statement.setLong(1, 5);

                try {
                    statement.addBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Add batch is only supported for insert values with parameter statement");
                }

                try {
                    statement.executeBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Execute batch is only supported for insert values with parameter statement");
                }

                boolean queryFlag = statement.execute();
                assertFalse(queryFlag);
                assertEquals(statement.getUpdateCount(), 2);

                try (Statement innerStatement = connection.createStatement();
                        ResultSet rs = innerStatement.executeQuery("SELECT count(*) from test_execute_batch_2")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 2);
                    //innerStatement.execute("DROP TABLE test_execute_batch_2");
                }
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_batch_2 SELECT * FROM test_execute_batch_non_insert_values where c_bigint = ?")) {
                statement.setLong(1, 6);

                try {
                    statement.addBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Add batch is only supported for insert values with parameter statement");
                }

                try {
                    statement.executeBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Execute batch is only supported for insert values with parameter statement");
                }

                boolean queryFlag = statement.execute();
                assertFalse(queryFlag);
                assertEquals(statement.getUpdateCount(), 1);

                try (Statement innerStatement = connection.createStatement();
                        ResultSet rs = innerStatement.executeQuery("SELECT count(*) from test_execute_batch_2")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 3);
                    innerStatement.execute("DROP TABLE test_execute_batch_2");
                }
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM test_execute_batch_non_insert_values where c_bigint = ?")) {
                statement.setLong(1, 5);

                try {
                    statement.addBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Add batch is only supported for insert values with parameter statement");
                }

                try {
                    statement.executeBatch();
                    fail("expected exception");
                }
                catch (SQLException e) {
                    assertEquals(e.getMessage(), "Execute batch is only supported for insert values with parameter statement");
                }

                boolean queryFlag = statement.execute();
                assertFalse(queryFlag);

                //Fixme: it should be 2 follows the java.sql interface, but hive haven't support returning the count
                assertEquals(statement.getUpdateCount(), 0);
            }

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT count(*) from test_execute_batch_non_insert_values")) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 1);
                statement.execute("DROP TABLE test_execute_batch_non_insert_values");
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        return createConnection("");
    }

    private Connection createConnection(String extra)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/hive/default?%s", server.getAddress(), extra);
        return DriverManager.getConnection(url, "admin", null);
    }
}
