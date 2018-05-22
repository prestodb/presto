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

import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import io.airlift.log.Logging;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static com.facebook.presto.jdbc.TestPrestoDriver.waitForNodeRefresh;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestJdbcPreparedStatement
{
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        waitForNodeRefresh(server);

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE SCHEMA blackhole.blackhole");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(server);
    }

    @Test
    public void testExecuteQuery()
            throws Exception
    {
        try (Connection connection = createConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")) {
            setTestValues(statement);

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertTestValues(rs);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testExecuteUpdate()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_execute_update (" +
                        "c_boolean boolean, " +
                        "c_tinyint tinyint, " +
                        "c_smallint smallint, " +
                        "c_integer integer, " +
                        "c_bigint bigint, " +
                        "c_real real, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_varbinary varbinary, " +
                        "c_date date, " +
                        "c_time time, " +
                        "c_timestamp timestamp," +
                        "c_null bigint)");
            }

            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO test_execute_update VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                setTestValues(statement);
                assertEquals(statement.executeUpdate(), 1);
            }
        }
    }

    @Test
    public void testPrepareMultiple()
            throws Exception
    {
        try (Connection connection = createConnection();
                PreparedStatement statement1 = connection.prepareStatement("SELECT 123");
                PreparedStatement statement2 = connection.prepareStatement("SELECT 456")) {
            try (ResultSet rs = statement1.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 123);
                assertFalse(rs.next());
            }

            try (ResultSet rs = statement2.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), 456);
                assertFalse(rs.next());
            }
        }
    }

    private static void setTestValues(PreparedStatement statement)
            throws SQLException
    {
        statement.setBoolean(1, true);
        statement.setByte(2, (byte) 2);
        statement.setShort(3, (short) 3);
        statement.setInt(4, 4);
        statement.setLong(5, 5L);
        statement.setFloat(6, 6.0f);
        statement.setDouble(7, 7.0d);
        statement.setBigDecimal(8, BigDecimal.valueOf(8L));
        statement.setString(9, "nine'nine\0\1\u2603\uD835\uDCABtest");
        statement.setBytes(10, "ten\0ten".getBytes(UTF_8));
        statement.setDate(11, new Date(11));
        statement.setTime(12, new Time(12));
        statement.setTimestamp(13, new Timestamp(13));
        statement.setNull(14, Types.BIGINT);
    }

    private static void assertTestValues(ResultSet rs)
            throws SQLException
    {
        assertTrue(rs.getBoolean(1));
        assertEquals(rs.getByte(2), (byte) 2);
        assertEquals(rs.getShort(3), (short) 3);
        assertEquals(rs.getInt(4), 4);
        assertEquals(rs.getLong(5), 5L);
        assertEquals(rs.getFloat(6), 6.0f);
        assertEquals(rs.getDouble(7), 7.0d);
        assertEquals(rs.getBigDecimal(8), BigDecimal.valueOf(8L));
        assertEquals(rs.getString(9), "nine'nine\0\1\u2603\uD835\uDCABtest");
        assertEquals(rs.getBytes(10), "ten\0ten".getBytes(UTF_8));
        assertEquals(rs.getDate(11).toString(), new Date(11).toString());
        assertEquals(rs.getTime(12).toString(), new Time(12).toString());
        assertEquals(rs.getTimestamp(13), new Timestamp(13));
        assertNull(rs.getObject(14));
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }
}
