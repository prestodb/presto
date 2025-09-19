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
import static java.lang.String.format;

public class ClpYamlMetadataProvider
        implements ClpMetadataProvider
{
    private static final Logger log = Logger.get(ClpYamlMetadataProvider.class);
    private final ClpConfig config;
    private Map<SchemaTableName, String> tableSchemaYamlMap;

    @Inject
    public ClpYamlMetadataProvider(ClpConfig config)
    {
        this.config = config;
    }

    @Override
    public List<ClpColumnHandle> listColumnHandles(SchemaTableName schemaTableName)
    {
        Path tableSchemaPath = Paths.get(tableSchemaYamlMap.get(schemaTableName));
        ClpSchemaTree schemaTree = new ClpSchemaTree(config.isPolymorphicTypeEnabled());
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            Map<String, Object> root = mapper.readValue(
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
        Path tablesSchemaPath = Paths.get(config.getMetadataYamlPath());
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try {
            Map<String, Object> root = mapper.readValue(new File(tablesSchemaPath.toString()),
                    new TypeReference<HashMap<String, Object>>() {});

            Object catalogObj = root.get(CONNECTOR_NAME);
            if (!(catalogObj instanceof Map)) {
                throw new PrestoException(CLP_UNSUPPORTED_TABLE_SCHEMA_YAML, format("The table schema does not contain field: %s", CONNECTOR_NAME));
            }
            Object schemaObj = ((Map<String, Object>) catalogObj).get(schemaName);
            ImmutableList.Builder<ClpTableHandle> tableHandlesBuilder = new ImmutableList.Builder<>();
            ImmutableMap.Builder<SchemaTableName, String> tableSchemaYamlMapBuilder = new ImmutableMap.Builder<>();
            for (Map.Entry<String, Object> schemaEntry : ((Map<String, Object>) schemaObj).entrySet()) {
                String tableName = schemaEntry.getKey();
                String tableSchemaYamlPath = schemaEntry.getValue().toString();
                // The splits' absolute paths will be stored in Pinot metadata database
                SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
                tableHandlesBuilder.add(new ClpTableHandle(schemaTableName, ""));
                tableSchemaYamlMapBuilder.put(schemaTableName, tableSchemaYamlPath);
            }
            this.tableSchemaYamlMap = tableSchemaYamlMapBuilder.build();
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
