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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_TABLE_SCHEMA_YAML;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static java.lang.String.format;

public class ClpYamlMetadataProvider
        implements ClpMetadataProvider
{
    private static final Logger log = Logger.get(ClpYamlMetadataProvider.class);
    private final ClpConfig config;
    private final ObjectMapper yamlMapper;

    // Thread-safe cache for schema names to avoid repeated file parsing
    private volatile List<String> cachedSchemaNames;

    // Thread-safe cache for table schema mappings per schema
    // Outer map: schema name -> inner map
    // Inner map: table name -> YAML schema file path
    private final Map<String, Map<String, String>> tableSchemaYamlMapPerSchema = new HashMap<>();

    @Inject
    public ClpYamlMetadataProvider(ClpConfig config)
    {
        this.config = config;
        // Reuse ObjectMapper instance for better performance
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    @Override
    public List<String> listSchemaNames()
    {
        // Use cached result if available to improve performance
        List<String> cached = cachedSchemaNames;
        if (cached != null) {
            return cached;
        }

        // Double-checked locking for thread-safe lazy initialization
        synchronized (this) {
            // Check again inside synchronized block
            cached = cachedSchemaNames;
            if (cached != null) {
                return cached;
            }

            // Check if YAML path is configured
            // If not configured, fall back to default schema for backward compatibility
            if (config.getMetadataYamlPath() == null) {
                log.warn("Metadata YAML path not configured, returning default schema only");
                cachedSchemaNames = ImmutableList.of(DEFAULT_SCHEMA_NAME);
                return cachedSchemaNames;
            }

            // Prepare to parse the YAML metadata file
            Path tablesSchemaPath = Paths.get(config.getMetadataYamlPath());

            try {
                // Parse the YAML file into a nested Map structure
                // Expected structure:
                //   clp:
                //     default:
                //       table1: /path/to/schema1.yaml
                //     dev:
                //       table2: /path/to/schema2.yaml
                Map<String, Object> root = yamlMapper.readValue(
                        new File(tablesSchemaPath.toString()),
                        new TypeReference<HashMap<String, Object>>() {});

                // Extract the catalog object (e.g., "clp")
                // This contains all schemas as keys
                Object catalogObj = root.get(CONNECTOR_NAME);
                if (!(catalogObj instanceof Map)) {
                    // Log error and fall back to default schema for graceful degradation
                    log.error("The metadata YAML does not contain valid catalog field: %s, returning default schema only", CONNECTOR_NAME);
                    List<String> defaultSchema = ImmutableList.of(DEFAULT_SCHEMA_NAME);
                    cachedSchemaNames = defaultSchema;
                    return defaultSchema;
                }

                // Extract schema names from the catalog Map
                // Each key represents a schema name (e.g., "default", "dev", "prod")
                Map<String, Object> catalogMap = (Map<String, Object>) catalogObj;
                List<String> schemas = ImmutableList.copyOf(catalogMap.keySet());
                log.info("Discovered %d schema(s) from YAML metadata: %s", schemas.size(), schemas);

                // Cache the result for future calls
                cachedSchemaNames = schemas;
                return schemas;
            }
            catch (IOException e) {
                // If YAML parsing fails (file not found, malformed, etc.), fall back to default schema
                // This ensures the connector still works even with configuration errors
                log.error(e, "Failed to parse metadata YAML file: %s, returning default schema only", config.getMetadataYamlPath());
                List<String> defaultSchema = ImmutableList.of(DEFAULT_SCHEMA_NAME);
                cachedSchemaNames = defaultSchema;
                return defaultSchema;
            }
        }
    }

    @Override
    public List<ClpColumnHandle> listColumnHandles(SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        // Get the schema-specific map
        Map<String, String> tablesInSchema;
        synchronized (tableSchemaYamlMapPerSchema) {
            tablesInSchema = tableSchemaYamlMapPerSchema.get(schemaName);
        }

        if (tablesInSchema == null) {
            log.error("No tables loaded for schema: %s", schemaName);
            return Collections.emptyList();
        }

        String schemaPath = tablesInSchema.get(tableName);
        if (schemaPath == null) {
            log.error("No schema path found for table: %s.%s", schemaName, tableName);
            return Collections.emptyList();
        }

        Path tableSchemaPath = Paths.get(schemaPath);
        ClpSchemaTree schemaTree = new ClpSchemaTree(config.isPolymorphicTypeEnabled());

        try {
            // Use the shared yamlMapper for better performance
            Map<String, Object> root = yamlMapper.readValue(
                    new File(tableSchemaPath.toString()),
                    new TypeReference<HashMap<String, Object>>() {});
            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Byte> typesBuilder = ImmutableList.builder();
            collectTypes(root, "", namesBuilder, typesBuilder);
            ImmutableList<String> names = namesBuilder.build();
            ImmutableList<Byte> types = typesBuilder.build();
            // The names and types should have same sizes
            for (int i = 0; i < names.size(); i++) {
                schemaTree.addColumn(names.get(i), types.get(i));
            }
            return schemaTree.collectColumnHandles();
        }
        catch (IOException e) {
            log.error(format("Failed to parse table schema file %s, error: %s", tableSchemaPath, e.getMessage()), e);
        }
        return Collections.emptyList();
    }

    @Override
    public List<ClpTableHandle> listTableHandles(String schemaName)
    {
        // Check if YAML path is configured
        if (config.getMetadataYamlPath() == null) {
            log.warn("Metadata YAML path not configured");
            return Collections.emptyList();
        }

        Path tablesSchemaPath = Paths.get(config.getMetadataYamlPath());

        try {
            // Use the shared yamlMapper for better performance
            Map<String, Object> root = yamlMapper.readValue(new File(tablesSchemaPath.toString()),
                    new TypeReference<HashMap<String, Object>>() {});

            Object catalogObj = root.get(CONNECTOR_NAME);
            if (!(catalogObj instanceof Map)) {
                throw new PrestoException(CLP_UNSUPPORTED_TABLE_SCHEMA_YAML, format("The table schema does not contain field: %s", CONNECTOR_NAME));
            }

            Object schemaObj = ((Map<String, Object>) catalogObj).get(schemaName);
            if (schemaObj == null) {
                log.warn("Schema '%s' not found in metadata YAML", schemaName);
                return Collections.emptyList();
            }

            if (!(schemaObj instanceof Map)) {
                log.error("Schema '%s' is not a valid map structure", schemaName);
                return Collections.emptyList();
            }

            ImmutableList.Builder<ClpTableHandle> tableHandlesBuilder = new ImmutableList.Builder<>();
            ImmutableMap.Builder<String, String> tableToYamlPathBuilder = new ImmutableMap.Builder<>();

            for (Map.Entry<String, Object> schemaEntry : ((Map<String, Object>) schemaObj).entrySet()) {
                String tableName = schemaEntry.getKey();
                String tableSchemaYamlPath = schemaEntry.getValue().toString();

                // Resolve relative paths relative to the directory containing tables-schema.yaml
                Path resolvedPath = Paths.get(tableSchemaYamlPath);
                if (!resolvedPath.isAbsolute()) {
                    // If relative, resolve it relative to the parent directory of tables-schema.yaml
                    Path parentDir = tablesSchemaPath.getParent();
                    if (parentDir != null) {
                        resolvedPath = parentDir.resolve(tableSchemaYamlPath).normalize();
                    }
                }

                // The splits' absolute paths will be stored in Pinot metadata database
                SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
                tableHandlesBuilder.add(new ClpTableHandle(schemaTableName, ""));
                tableToYamlPathBuilder.put(tableName, resolvedPath.toString());
            }

            // Thread-safe update of the schema-specific table map
            synchronized (tableSchemaYamlMapPerSchema) {
                tableSchemaYamlMapPerSchema.put(schemaName, tableToYamlPathBuilder.build());
            }

            return tableHandlesBuilder.build();
        }
        catch (IOException e) {
            log.error(format("Failed to parse metadata file: %s, error: %s", config.getMetadataYamlPath(), e.getMessage()), e);
        }
        return Collections.emptyList();
    }

    private void collectTypes(Object node, String prefix, ImmutableList.Builder<String> namesBuilder, ImmutableList.Builder<Byte> typesBuilder)
    {
        if (node instanceof Number) {
            namesBuilder.add(prefix);
            typesBuilder.add(((Number) node).byteValue());
            return;
        }
        if (node instanceof List) {
            for (Number type : (List<Number>) node) {
                namesBuilder.add(prefix);
                typesBuilder.add(type.byteValue());
            }
            return;
        }
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) node).entrySet()) {
            if (!prefix.isEmpty()) {
                collectTypes(entry.getValue(), format("%s.%s", prefix, entry.getKey()), namesBuilder, typesBuilder);
                continue;
            }
            collectTypes(entry.getValue(), entry.getKey(), namesBuilder, typesBuilder);
        }
    }
}
