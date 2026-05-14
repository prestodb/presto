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
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestConnectionValidation
{
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TESTING_CATALOG, "tpch");
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
    }

    @Test
    public void testValidConnectionWithValidationEnabled()
            throws SQLException
    {
        // Test validation via URL parameter
        String url = String.format("jdbc:presto://%s?validateConnection=true", server.getAddress());
        try (Connection connection = DriverManager.getConnection(url, "test", null)) {
            assertNotNull(connection);
            assertTrue(connection.isValid(10));
        }
    }

    @Test
    public void testValidConnectionWithValidationEnabledViaProperties()
            throws SQLException
    {
        // Test validation via Properties
        String url = String.format("jdbc:presto://%s", server.getAddress());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("validateConnection", "true");

        try (Connection connection = DriverManager.getConnection(url, properties)) {
            assertNotNull(connection);
            assertTrue(connection.isValid(10));
        }
    }

    @Test
    public void testValidConnectionWithValidationDisabled()
            throws SQLException
    {
        // Test default behavior (validation disabled)
        String url = String.format("jdbc:presto://%s", server.getAddress());
        try (Connection connection = DriverManager.getConnection(url, "test", null)) {
            assertNotNull(connection);
            assertTrue(connection.isValid(10));
        }
    }

    @Test
    public void testValidConnectionWithValidationExplicitlyDisabled()
            throws SQLException
    {
        // Test explicitly disabled validation
        String url = String.format("jdbc:presto://%s?validateConnection=false", server.getAddress());
        try (Connection connection = DriverManager.getConnection(url, "test", null)) {
            assertNotNull(connection);
            assertTrue(connection.isValid(10));
        }
    }

    @Test
    public void testInvalidConnectionWithValidationEnabled()
    {
        // Test validation failure with invalid server
        String url = "jdbc:presto://invalid-host:8080?validateConnection=true";
        try {
            DriverManager.getConnection(url, "test", null);
            fail("Expected SQLException due to connection validation failure");
        }
        catch (SQLException e) {
            // Expected - connection validation should fail
            assertTrue(e.getMessage().contains("Connection validation failed") ||
                      e.getMessage().contains("Connection refused") ||
                      e.getMessage().contains("UnknownHostException"),
                    "Expected connection validation error, got: " + e.getMessage());
        }
    }

    @Test
    public void testValidationWithCatalogAndSchema()
            throws SQLException
    {
        // Test validation with catalog and schema specified
        String url = String.format("jdbc:presto://%s/%s/tiny?validateConnection=true",
                server.getAddress(), TESTING_CATALOG);
        try (Connection connection = DriverManager.getConnection(url, "test", null)) {
            assertNotNull(connection);
            assertTrue(connection.isValid(10));
        }
    }

    @Test
    public void testValidationWithSessionProperties()
            throws SQLException
    {
        // Test validation with session properties
        String url = String.format("jdbc:presto://%s?validateConnection=true&sessionProperties=query_max_memory:1GB",
                server.getAddress());
        try (Connection connection = DriverManager.getConnection(url, "test", null)) {
            assertNotNull(connection);
            assertTrue(connection.isValid(10));
        }
    }

    @Test
    public void testValidationQueryExecution()
            throws SQLException
    {
        // Verify that validation doesn't interfere with normal query execution
        String url = String.format("jdbc:presto://%s/%s/tiny?validateConnection=true",
                server.getAddress(), TESTING_CATALOG);
        try (Connection connection = DriverManager.getConnection(url, "test", null)) {
            assertNotNull(connection);

            // Execute a simple query to ensure connection works after validation
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == 1);
            }
        }
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        try {
            if (closeable != null) {
                closeable.close();
            }
        }
        catch (Exception ignored) {
        }
    }
}
