package com.facebook.presto.jdbc;

import com.facebook.presto.server.TestingPrestoServer;
import com.google.common.io.Closeables;
import com.google.common.net.HostAndPort;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
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

public class TestDriver
{
    private TestingPrestoServer server;
    private HostAndPort address;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        URI uri = server.getBaseUrl();
        address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
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
        String url = format("jdbc:presto://%s/", address);
        try (Connection connection = DriverManager.getConnection(url)) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                assertRowCount(tableTypes, 1);
            }

            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("select 123 x, 'foo' y from dual")) {
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
