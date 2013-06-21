package com.facebook.presto.jdbc;

import com.facebook.presto.server.TestingPrestoServer;
import com.google.common.io.Closeables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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

    @SuppressWarnings("deprecation")
    @AfterMethod
    public void teardown()
    {
        Closeables.closeQuietly(server);
    }

    @Test
    public void testDriverManager()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                assertRowCount(tableTypes, 1);
            }

            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT 123 x, 'foo' y FROM dual")) {
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

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = ".* Table .* does not exist")
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

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s/", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private static void assertRowCount(ResultSet rs, int expected)
            throws SQLException
    {
        int actual = 0;
        while (rs.next()) {
            actual++;
        }
        assertEquals(actual, expected);
    }
}
