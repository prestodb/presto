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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpYamlMetadataProvider;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType.YAML;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestClpMultiSchema
{
    @Test
    public void testSingleSchemaDiscovery() throws IOException
    {
        // Create a temporary YAML file with single schema
        File tempFile = File.createTempFile("clp-metadata-single-", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("clp:\n");
            writer.write("  default:\n");
            writer.write("    table1: /path/to/table1.yaml\n");
            writer.write("    table2: /path/to/table2.yaml\n");
        }

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(tempFile.getAbsolutePath());

        ClpMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    @Test
    public void testMultiSchemaDiscovery() throws IOException
    {
        // Create a temporary YAML file with multiple schemas
        File tempFile = File.createTempFile("clp-metadata-multi-", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("clp:\n");
            writer.write("  default:\n");
            writer.write("    logs_table: /path/to/default/logs.yaml\n");
            writer.write("  dev:\n");
            writer.write("    test_logs: /path/to/dev/test.yaml\n");
            writer.write("  prod:\n");
            writer.write("    production_logs: /path/to/prod/logs.yaml\n");
        }

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(tempFile.getAbsolutePath());

        ClpMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 3);
        Set<String> schemaSet = ImmutableSet.copyOf(schemas);
        assertTrue(schemaSet.contains("default"));
        assertTrue(schemaSet.contains("dev"));
        assertTrue(schemaSet.contains("prod"));
    }

    @Test
    public void testMissingYamlPathReturnsDefault()
    {
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML);
        // Note: not setting metadataYamlPath

        ClpMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    @Test
    public void testInvalidYamlPathReturnsDefault()
    {
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath("/nonexistent/path/to/metadata.yaml");

        ClpMetadataProvider provider = new ClpYamlMetadataProvider(config);
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }
}
