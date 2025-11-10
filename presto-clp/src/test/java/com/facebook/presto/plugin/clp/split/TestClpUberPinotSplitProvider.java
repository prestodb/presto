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
package com.facebook.presto.plugin.clp.split;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for ClpUberPinotSplitProvider.
 * Tests Uber-specific customizations including Neutrino endpoint URL construction
 * and RTA table name prefixing.
 */
@Test(singleThreaded = true)
public class TestClpUberPinotSplitProvider
{
    private ClpUberPinotSplitProvider splitProvider;
    private ClpConfig config;

    @BeforeMethod
    public void setUp()
    {
        config = new ClpConfig();
        config.setMetadataDbUrl("https://neutrino.uber.com");
        config.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_UBER);
        splitProvider = new ClpUberPinotSplitProvider(config);
    }

    /**
     * Test that the Neutrino endpoint URL is correctly constructed.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrl() throws Exception
    {
        // Use reflection to access the protected method
        Method method = ClpUberPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        URL result = (URL) method.invoke(splitProvider, config);

        assertNotNull(result);
        assertEquals(result.toString(), "https://neutrino.uber.com/v1/globalStatements");
        assertEquals(result.getProtocol(), "https");
        assertEquals(result.getHost(), "neutrino.uber.com");
        assertEquals(result.getPath(), "/v1/globalStatements");
    }

    /**
     * Test URL construction with different base URLs.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlVariations() throws Exception
    {
        Method method = ClpUberPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        // Test with trailing slash
        config.setMetadataDbUrl("https://neutrino.uber.com/");
        URL result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://neutrino.uber.com//v1/globalStatements");

        // Test without protocol (should work as URL constructor handles it)
        config.setMetadataDbUrl("http://neutrino-dev.uber.com");
        result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "http://neutrino-dev.uber.com/v1/globalStatements");

        // Test with port
        config.setMetadataDbUrl("https://neutrino.uber.com:8080");
        result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://neutrino.uber.com:8080/v1/globalStatements");
    }

    /**
     * Test that invalid URLs throw MalformedURLException.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlInvalid() throws Exception
    {
        Method method = ClpUberPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        config.setMetadataDbUrl("not a valid url");
        try {
            method.invoke(splitProvider, config);
            fail("Expected MalformedURLException");
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof MalformedURLException);
        }
    }

    /**
     * Test that table names are correctly prefixed with "rta.logging."
     */
    @Test
    public void testInferMetadataTableName()
    {
        SchemaTableName schemaTableName = new SchemaTableName("default", "logs");
        ClpTableHandle tableHandle = new ClpTableHandle(schemaTableName, "test");

        String result = splitProvider.inferMetadataTableName(tableHandle);

        assertEquals(result, "rta.logging.logs");
    }

    /**
     * Test table name inference with different schemas.
     * Verifies that schema name doesn't affect the output (flat namespace).
     */
    @Test
    public void testInferMetadataTableNameDifferentSchemas()
    {
        // Test with default schema
        SchemaTableName schemaTableName1 = new SchemaTableName("default", "events");
        ClpTableHandle tableHandle1 = new ClpTableHandle(schemaTableName1, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle1), "rta.logging.events");

        // Test with production schema - should produce same result
        SchemaTableName schemaTableName2 = new SchemaTableName("production", "events");
        ClpTableHandle tableHandle2 = new ClpTableHandle(schemaTableName2, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle2), "rta.logging.events");

        // Test with staging schema
        SchemaTableName schemaTableName3 = new SchemaTableName("staging", "metrics");
        ClpTableHandle tableHandle3 = new ClpTableHandle(schemaTableName3, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle3), "rta.logging.metrics");
    }

    /**
     * Test table name inference with special characters.
     */
    @Test
    public void testInferMetadataTableNameSpecialCharacters()
    {
        // Test with underscore
        SchemaTableName schemaTableName1 = new SchemaTableName("default", "user_logs");
        ClpTableHandle tableHandle1 = new ClpTableHandle(schemaTableName1, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle1), "rta.logging.user_logs");

        // Test with hyphen
        SchemaTableName schemaTableName2 = new SchemaTableName("default", "app-logs");
        ClpTableHandle tableHandle2 = new ClpTableHandle(schemaTableName2, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle2), "rta.logging.app-logs");

        // Test with numbers
        SchemaTableName schemaTableName3 = new SchemaTableName("default", "logs2024");
        ClpTableHandle tableHandle3 = new ClpTableHandle(schemaTableName3, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle3), "rta.logging.logs2024");
    }

    /**
     * Test that null table handle throws NullPointerException.
     */
    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "tableHandle is null")
    public void testInferMetadataTableNameNull()
    {
        splitProvider.inferMetadataTableName(null);
    }

    /**
     * Test the factory method for building Uber table names.
     */
    @Test
    public void testBuildUberTableName()
    {
        assertEquals(splitProvider.buildUberTableName("logs"), "rta.logging.logs");
        assertEquals(splitProvider.buildUberTableName("events"), "rta.logging.events");
        assertEquals(splitProvider.buildUberTableName("metrics"), "rta.logging.metrics");
        assertEquals(splitProvider.buildUberTableName("user_activity"), "rta.logging.user_activity");
        assertEquals(splitProvider.buildUberTableName("app-logs"), "rta.logging.app-logs");
    }

    /**
     * Test that the split provider is correctly instantiated with configuration.
     */
    @Test
    public void testConstructor()
    {
        assertNotNull(splitProvider);

        // Verify it's an instance of the parent class
        assertTrue(splitProvider instanceof ClpPinotSplitProvider);
        assertTrue(splitProvider instanceof ClpSplitProvider);
    }

    /**
     * Test SQL query building methods inherited from parent.
     */
    @Test
    public void testInheritedSqlQueryMethods()
    {
        // Test buildSplitSelectionQuery (inherited from parent)
        String query = splitProvider.buildSplitSelectionQuery("rta.logging.logs", "status = 200");
        assertTrue(query.contains("rta.logging.logs"));
        assertTrue(query.contains("status = 200"));
        assertTrue(query.contains("SELECT"));
        assertTrue(query.contains("tpath"));

        // Test buildSplitMetadataQuery (inherited from parent)
        String metaQuery = splitProvider.buildSplitMetadataQuery("rta.logging.events", "timestamp > 1000", "timestamp", "DESC");
        assertTrue(metaQuery.contains("rta.logging.events"));
        assertTrue(metaQuery.contains("timestamp > 1000"));
        assertTrue(metaQuery.contains("ORDER BY timestamp DESC"));
        assertTrue(metaQuery.contains("creationtime"));
        assertTrue(metaQuery.contains("lastmodifiedtime"));
        assertTrue(metaQuery.contains("num_messages"));
    }

    /**
     * Test configuration with different split provider types.
     */
    @Test
    public void testConfigurationTypes()
    {
        // Test that the configuration is set correctly
        assertEquals(config.getSplitProviderType(), ClpConfig.SplitProviderType.PINOT_UBER);

        // Create a new instance with different config to ensure isolation
        ClpConfig newConfig = new ClpConfig();
        newConfig.setMetadataDbUrl("https://other-neutrino.uber.com");
        newConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_UBER);

        ClpUberPinotSplitProvider newProvider = new ClpUberPinotSplitProvider(newConfig);
        assertNotNull(newProvider);
    }
}
