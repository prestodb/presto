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
package com.facebook.presto.plugin.clp.metadata;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType.YAML;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpYamlMetadataProvider
{
    private final List<File> tempFiles = new ArrayList<>();

    @AfterClass
    public void cleanup()
    {
        // Clean up temporary files
        for (File file : tempFiles) {
            if (file.exists()) {
                file.delete();
            }
        }
    }

    /**
     * Test that listSchemaNames returns only the default schema when YAML has single schema
     */
    @Test
    public void testListSchemaNamesSingleSchema() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    table1: /path/to/table1.yaml\n" +
                "    table2: /path/to/table2.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames discovers multiple schemas from YAML
     */
    @Test
    public void testListSchemaNamesMultipleSchemas() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    logs: /path/to/default/logs.yaml\n" +
                "  dev:\n" +
                "    test_logs: /path/to/dev/logs.yaml\n" +
                "  staging:\n" +
                "    staging_logs: /path/to/staging/logs.yaml\n" +
                "  prod:\n" +
                "    production_logs: /path/to/prod/logs.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 4);
        Set<String> schemaSet = ImmutableSet.copyOf(schemas);
        assertTrue(schemaSet.contains("default"));
        assertTrue(schemaSet.contains("dev"));
        assertTrue(schemaSet.contains("staging"));
        assertTrue(schemaSet.contains("prod"));
    }

    /**
     * Test that listSchemaNames handles missing YAML path gracefully
     */
    @Test
    public void testListSchemaNamesNullPath()
    {
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML);
        // Note: not setting metadataYamlPath

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames handles nonexistent file gracefully
     */
    @Test
    public void testListSchemaNamesNonexistentFile()
    {
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath("/nonexistent/path/metadata.yaml");

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames handles malformed YAML gracefully
     */
    @Test
    public void testListSchemaNamesMalformedYaml() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "this is not\n" +
                "  valid: yaml: content\n" +
                "    - with random structure\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames handles YAML without catalog field
     */
    @Test
    public void testListSchemaNamesNoCatalogField() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "some_other_catalog:\n" +
                "  default:\n" +
                "    table1: /path/to/table1.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        // Should fall back to default schema on error
        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listTableHandles returns correct tables for a schema
     */
    @Test
    public void testListTableHandles() throws IOException
    {
        // Create schema YAML files
        File table1Schema = createTempYamlFile("column1: 1\ncolumn2: 2\n");
        File table2Schema = createTempYamlFile("field1: 3\nfield2: 4\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    table1: " + table1Schema.getAbsolutePath() + "\n" +
                "    table2: " + table2Schema.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<ClpTableHandle> tables = provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        assertEquals(tables.size(), 2);
        Set<String> tableNames = ImmutableSet.of(
                tables.get(0).getSchemaTableName().getTableName(),
                tables.get(1).getSchemaTableName().getTableName());
        assertTrue(tableNames.contains("table1"));
        assertTrue(tableNames.contains("table2"));
    }

    /**
     * Test that listTableHandles returns correct tables for multiple schemas
     */
    @Test
    public void testListTableHandlesMultipleSchemas() throws IOException
    {
        File devTable = createTempYamlFile("col: 1\n");
        File prodTable = createTempYamlFile("col: 2\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  dev:\n" +
                "    dev_logs: " + devTable.getAbsolutePath() + "\n" +
                "  prod:\n" +
                "    prod_logs: " + prodTable.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);

        // Test dev schema
        List<ClpTableHandle> devTables = provider.listTableHandles("dev");
        assertEquals(devTables.size(), 1);
        assertEquals(devTables.get(0).getSchemaTableName().getTableName(), "dev_logs");
        assertEquals(devTables.get(0).getSchemaTableName().getSchemaName(), "dev");

        // Test prod schema
        List<ClpTableHandle> prodTables = provider.listTableHandles("prod");
        assertEquals(prodTables.size(), 1);
        assertEquals(prodTables.get(0).getSchemaTableName().getTableName(), "prod_logs");
        assertEquals(prodTables.get(0).getSchemaTableName().getSchemaName(), "prod");
    }

    /**
     * Test that schema names are returned in consistent order
     */
    @Test
    public void testSchemaNameConsistency() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  schema_a:\n" +
                "    table: /path/a.yaml\n" +
                "  schema_b:\n" +
                "    table: /path/b.yaml\n" +
                "  schema_c:\n" +
                "    table: /path/c.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);

        // Call multiple times to verify consistency
        List<String> schemas1 = provider.listSchemaNames();
        List<String> schemas2 = provider.listSchemaNames();
        List<String> schemas3 = provider.listSchemaNames();

        assertEquals(schemas1, schemas2);
        assertEquals(schemas2, schemas3);
    }

    /**
     * Test empty schema (no tables)
     */
    @Test
    public void testEmptySchema() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  empty_schema:\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertTrue(schemas.contains("empty_schema"));

        List<ClpTableHandle> tables = provider.listTableHandles("empty_schema");
        assertTrue(tables.isEmpty());
    }

    /**
     * Test that schemas with special characters in names are handled
     */
    @Test
    public void testSchemaWithSpecialCharacters() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  schema_with_underscores:\n" +
                "    table: /path/table.yaml\n" +
                "  schema-with-dashes:\n" +
                "    table: /path/table2.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 2);
        Set<String> schemaSet = ImmutableSet.copyOf(schemas);
        assertTrue(schemaSet.contains("schema_with_underscores"));
        assertTrue(schemaSet.contains("schema-with-dashes"));
    }

    /**
     * Helper method to create temporary YAML files for testing
     */
    private File createTempYamlFile(String content) throws IOException
    {
        File tempFile = Files.createTempFile("clp-test-", ".yaml").toFile();
        tempFile.deleteOnExit();
        tempFiles.add(tempFile);

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(content);
        }

        return tempFile;
    }
}
