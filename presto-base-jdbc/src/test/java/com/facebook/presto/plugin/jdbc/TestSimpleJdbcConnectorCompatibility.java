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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test to ensure that JDBC connectors can work without depending on BaseJdbcConfig.
 * This verifies continued compatibility for connectors that implement their own
 * configuration mechanisms.
 */
@Test(singleThreaded = true)
public class TestSimpleJdbcConnectorCompatibility
{
    private TestingDatabase database;
    private JdbcMetadata metadata;
    private JdbcMetadataCache jdbcMetadataCache;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        ListeningExecutorService executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s")));
        jdbcMetadataCache = new JdbcMetadataCache(executor, database.getJdbcClient(), new JdbcMetadataCacheStats(), OptionalLong.of(0), OptionalLong.of(0), 100);

        // Create a simple config that doesn't extend BaseJdbcConfig
        SimpleTestJdbcConfig simpleConfig = new SimpleTestJdbcConfig()
                .setJdbcUrl("jdbc:h2:mem:test")
                .setUsername("test")
                .setPassword("test");

        // Create a TableLocationProvider that uses the simple config
        SimpleTestTableLocationProvider locationProvider = new SimpleTestTableLocationProvider(simpleConfig);

        // Create JdbcMetadata with the simple provider (not using BaseJdbcConfig)
        metadata = new JdbcMetadata(jdbcMetadataCache, database.getJdbcClient(), false, locationProvider);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testSimpleConnectorBasicOperations()
    {
        // Verify that basic metadata operations work with a connector that doesn't use BaseJdbcConfig

        // Test schema listing
        assertTrue(metadata.listSchemaNames(SESSION).containsAll(ImmutableSet.of("example", "tpch")));

        // Test table handle retrieval
        JdbcTableHandle tableHandle = metadata.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
        assertNotNull(tableHandle);

        // Test table metadata retrieval
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("example", "numbers"));
        assertEquals(tableMetadata.getColumns().size(), 3);

        // Test column handles
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);
        assertEquals(columnHandles.size(), 3);
        assertTrue(columnHandles.containsKey("text"));
        assertTrue(columnHandles.containsKey("text_short"));
        assertTrue(columnHandles.containsKey("value"));

        // Test table listing
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("example"))), ImmutableSet.of(
                new SchemaTableName("example", "numbers"),
                new SchemaTableName("example", "view_source"),
                new SchemaTableName("example", "view")));
    }

    @Test
    public void testSimpleTableLocationProvider()
    {
        // Create a simple config
        SimpleTestJdbcConfig config = new SimpleTestJdbcConfig()
                .setJdbcUrl("jdbc:test://custom-location:9999/testdb");

        // Create the location provider
        SimpleTestTableLocationProvider provider = new SimpleTestTableLocationProvider(config);

        // Verify it returns the expected location
        assertEquals(provider.getTableLocation(), "jdbc:test://custom-location:9999/testdb");
    }

    @Test
    public void testConfigurationIndependence()
    {
        // Verify that SimpleTestJdbcConfig works independently of BaseJdbcConfig
        SimpleTestJdbcConfig config = new SimpleTestJdbcConfig()
                .setJdbcUrl("jdbc:postgresql://localhost:5432/testdb")
                .setUsername("testuser")
                .setPassword("testpass");

        assertEquals(config.getJdbcUrl(), "jdbc:postgresql://localhost:5432/testdb");
        assertEquals(config.getUsername(), "testuser");
        assertEquals(config.getPassword(), "testpass");

        // Verify that this config can be used to create a working TableLocationProvider
        SimpleTestTableLocationProvider provider = new SimpleTestTableLocationProvider(config);
        assertEquals(provider.getTableLocation(), "jdbc:postgresql://localhost:5432/testdb");
    }
}
