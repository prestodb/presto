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

import com.facebook.presto.server.testing.TestingPrestoServer;
import io.airlift.log.Logging;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
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

import static com.facebook.presto.jdbc.TestDriver.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcResultSet
{
    private TestingPrestoServer server;

    private Connection connection;
    private Statement statement;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
    }

    @AfterClass
    public void teardownServer()
    {
        closeQuietly(server);
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
        statement = connection.createStatement();
    }

    @AfterMethod
    public void teardown()
    {
        closeQuietly(statement);
        closeQuietly(connection);
    }

    @Test
    public void testDuplicateColumnLabels()
            throws Exception
    {
        try (ResultSet rs = statement.executeQuery("SELECT 123 x, 456 x")) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 2);
            assertEquals(metadata.getColumnName(1), "x");
            assertEquals(metadata.getColumnName(2), "x");

            assertTrue(rs.next());
            assertEquals(rs.getLong(1), 123L);
            assertEquals(rs.getLong(2), 456L);
            assertEquals(rs.getLong("x"), 123L);
        }
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    public void testObjectTypes()
            throws Exception
    {
        String sql = "SELECT 123, 12300000000, 0.1, true, 'hello', 1.0 / 0.0, 0.0 / 0.0, ARRAY[1, 2]";
        try (ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 8);
            assertEquals(metadata.getColumnType(1), Types.INTEGER);
            assertEquals(metadata.getColumnType(2), Types.BIGINT);
            assertEquals(metadata.getColumnType(3), Types.DOUBLE);
            assertEquals(metadata.getColumnType(4), Types.BOOLEAN);
            assertEquals(metadata.getColumnType(5), Types.LONGNVARCHAR);
            assertEquals(metadata.getColumnType(6), Types.DOUBLE);
            assertEquals(metadata.getColumnType(7), Types.DOUBLE);
            assertEquals(metadata.getColumnType(8), Types.ARRAY);

            assertTrue(rs.next());
            assertEquals(rs.getObject(1), 123);
            assertEquals(rs.getObject(2), 12300000000L);
            assertEquals(rs.getObject(3), 0.1d);
            assertEquals(rs.getObject(4), true);
            assertEquals(rs.getObject(5), "hello");
            assertEquals(rs.getObject(6), Double.POSITIVE_INFINITY);
            assertEquals(rs.getObject(7), Double.NaN);
            assertEquals(rs.getArray(8).getArray(), new int[] {1, 2});
        }
    }

    @Test
    public void testStatsExtraction()
            throws Exception
    {
        try (PrestoResultSet rs = (PrestoResultSet) statement.executeQuery("SELECT 123 x, 456 x")) {
            assertNotNull(rs.getStats());
            assertTrue(rs.next());
            assertNotNull(rs.getStats());
            assertFalse(rs.next());
            assertNotNull(rs.getStats());
        }
    }

    @Test(expectedExceptions = SQLFeatureNotSupportedException.class, expectedExceptionsMessageRegExp = "SET/RESET SESSION .*")
    public void testSetSession()
        throws Exception
    {
        statement.execute("SET SESSION hash_partition_count = 16");
    }

    @Test(expectedExceptions = SQLFeatureNotSupportedException.class, expectedExceptionsMessageRegExp = "SET/RESET SESSION .*")
    public void testResetSession()
        throws Exception
    {
        statement.execute("RESET SESSION hash_partition_count");
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }
}
