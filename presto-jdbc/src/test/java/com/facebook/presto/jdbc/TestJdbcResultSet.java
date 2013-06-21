package com.facebook.presto.jdbc;

import com.facebook.presto.server.TestingPrestoServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.jdbc.TestDriver.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJdbcResultSet
{
    private TestingPrestoServer server;

    private Connection connection;
    private Statement statement;

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        connection = createConnection();
        statement = connection.createStatement();
    }

    @AfterMethod
    public void teardown()
    {
        closeQuietly(statement);
        closeQuietly(connection);
        closeQuietly(server);
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

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s/", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }
}
