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
package com.facebook.presto.plugin.opensearch;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.opensearch.types.TypeMapper;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Objects.requireNonNull;

/**
 * Metadata provider for OpenSearch connector.
 * Handles schema/table/column discovery and metadata operations.
 * Supports nested field discovery when enabled in configuration.
 */
public class OpenSearchMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(OpenSearchMetadata.class);
    private static final String DEFAULT_SCHEMA = "default";

    private final OpenSearchClient client;
    private final OpenSearchConfig config;
    private final NestedFieldMapper nestedFieldMapper;

    @Inject
    public OpenSearchMetadata(
            OpenSearchClient client,
            OpenSearchConfig config,
            NestedFieldMapper nestedFieldMapper)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
        this.nestedFieldMapper = requireNonNull(nestedFieldMapper, "nestedFieldMapper is null");
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof KnnSearchTableFunction.KnnSearchHandle) {
            KnnSearchTableFunction.KnnSearchHandle knnHandle = (KnnSearchTableFunction.KnnSearchHandle) handle;

            // Create a table handle for the k-NN search with search parameters
            OpenSearchTableHandle tableHandle = new OpenSearchTableHandle(
                    DEFAULT_SCHEMA,
                    knnHandle.getIndexName(),
                    knnHandle.getIndexName(),
                    knnHandle.getVectorField(),
                    knnHandle.getQueryVector(),
                    knnHandle.getK(),
                    knnHandle.getSpaceType(),
                    knnHandle.getEfSearch());

            // Return the table handle with columns from the k-NN search
            return Optional.of(new TableFunctionApplicationResult<>(
                    tableHandle,
                    new ArrayList<>(knnHandle.getColumns())));
        }
        return Optional.empty();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        // For OpenSearch, we use a single schema "default"
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!DEFAULT_SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }

        // Verify the index exists
        List<String> indices = client.listIndices();
        if (!indices.contains(tableName.getTableName())) {
            return null;
        }

        // Table name corresponds to index name
        return new OpenSearchTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        OpenSearchTableHandle tableHandle = (OpenSearchTableHandle) table;

        // For k-NN searches, build column metadata from index mapping
        if (tableHandle.isKnnSearch()) {
            Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
            List<ColumnMetadata> columns = new ArrayList<>();
            for (Map.Entry<String, ColumnHandle> entry : columnHandles.entrySet()) {
                OpenSearchColumnHandle columnHandle = (OpenSearchColumnHandle) entry.getValue();
                columns.add(ColumnMetadata.builder()
                        .setName(columnHandle.getColumnName())
                        .setType(columnHandle.getColumnType())
                        .build());
            }
            return new ConnectorTableMetadata(tableHandle.toSchemaTableName(), columns);
        }

        List<ColumnMetadata> columns = getColumnsFromMapping(tableHandle.getIndexName());
        return new ConnectorTableMetadata(tableHandle.toSchemaTableName(), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !DEFAULT_SCHEMA.equals(schemaName.get())) {
            return ImmutableList.of();
        }

        List<String> indices = client.listIndices();
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();

        for (String index : indices) {
            tables.add(new SchemaTableName(DEFAULT_SCHEMA, index));
        }

        List<SchemaTableName> result = tables.build();
        log.debug("Listed %d tables", result.size());
        return result;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        OpenSearchTableHandle table = (OpenSearchTableHandle) tableHandle;
        Map<String, Object> mapping = client.getIndexMapping(table.getIndexName());

        // For k-NN searches, build columns from index mapping (including _id and _score)
        if (table.isKnnSearch()) {
            Map<String, ColumnHandle> columns = new LinkedHashMap<>();
            columns.put("_id", new OpenSearchColumnHandle("_id", VARCHAR, "keyword"));
            columns.put("_score", new OpenSearchColumnHandle("_score", DOUBLE, "double"));

            // Add other fields from mapping (skip knn_vector fields)
            for (Map.Entry<String, Object> entry : mapping.entrySet()) {
                String fieldName = entry.getKey();
                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
                    Object typeObj = fieldMapping.get("type");
                    if (typeObj instanceof String) {
                        String fieldType = (String) typeObj;
                        if (!"knn_vector".equals(fieldType)) {
                            columns.put(fieldName, new OpenSearchColumnHandle(fieldName, VARCHAR, fieldType));
                        }
                    }
                }
            }
            return columns;
        }

        // Use LinkedHashMap to preserve insertion order, then sort by column name
        Map<String, ColumnHandle> columns = new LinkedHashMap<>();

        // Add _id column unless configured to hide it
        if (!config.isHideDocumentIdColumn()) {
            columns.put("_id", new OpenSearchColumnHandle("_id", VARCHAR, "keyword"));
        }

        // Check if nested field access is enabled
        if (config.isNestedFieldAccessEnabled()) {
            // Use NestedFieldMapper to discover all fields including nested ones
            Map<String, NestedFieldInfo> allFields = nestedFieldMapper.discoverNestedFields(mapping);

            // Add ALL fields as columns (both parent objects and leaf fields)
            // Parent objects will be exposed as ROW types for dereference support
            // Leaf fields will be exposed with their proper types
            for (Map.Entry<String, NestedFieldInfo> entry : allFields.entrySet()) {
                NestedFieldInfo fieldInfo = entry.getValue();

                // Use the Presto type from NestedFieldInfo
                // For parent objects, this will be a ROW type
                // For leaf fields, this will be the primitive type
                Type columnType = fieldInfo.getPrestoType();

                // Check if it's a vector field
                boolean isVector = "knn_vector".equals(fieldInfo.getOpenSearchType());
                int vectorDimension = 0;
                if (isVector) {
                    // Vector dimension would be in the original mapping
                    Map<String, Object> fieldProps = getFieldProperties(mapping, fieldInfo.getFieldPathList());
                    if (fieldProps != null && fieldProps.containsKey("dimension")) {
                        vectorDimension = ((Number) fieldProps.get("dimension")).intValue();
                    }
                }

                columns.put(fieldInfo.getFieldPath(), new OpenSearchColumnHandle(
                        fieldInfo.getFieldPath(),
                        columnType,
                        fieldInfo.getOpenSearchType(),
                        isVector,
                        vectorDimension,
                        fieldInfo.isNestedField(),
                        fieldInfo.getParentPath(),
                        fieldInfo.getFieldPathList(),
                        fieldInfo.getJsonPath()));
            }
        }
        else {
            // Original flat field logic (backward compatibility)
            for (Map.Entry<String, Object> entry : mapping.entrySet()) {
                String fieldName = entry.getKey();
                Map<String, Object> fieldProperties = (Map<String, Object>) entry.getValue();

                String fieldType = (String) fieldProperties.getOrDefault("type", "text");
                Type prestoType = TypeMapper.toPrestoType(fieldType, fieldProperties, fieldName);

                // Check if it's a vector field
                boolean isVector = "knn_vector".equals(fieldType);
                int vectorDimension = 0;
                if (isVector && fieldProperties.containsKey("dimension")) {
                    vectorDimension = ((Number) fieldProperties.get("dimension")).intValue();
                }

                columns.put(fieldName, new OpenSearchColumnHandle(
                        fieldName,
                        prestoType,
                        fieldType,
                        isVector,
                        vectorDimension));
            }
        }

        log.debug("Retrieved %d columns for table %s", columns.size(), table.getTableName());
        return columns;
    }

    /**
     * Helper method to get field properties from mapping using a field path.
     * Used to retrieve additional properties like vector dimension for nested fields.
     */
    private Map<String, Object> getFieldProperties(Map<String, Object> mapping, List<String> fieldPath)
    {
        if (fieldPath == null || fieldPath.isEmpty()) {
            return null;
        }

        Map<String, Object> current = mapping;

        for (int i = 0; i < fieldPath.size(); i++) {
            String pathSegment = fieldPath.get(i);

            if (!current.containsKey(pathSegment)) {
                return null;
            }

            Object value = current.get(pathSegment);
            if (!(value instanceof Map)) {
                return null;
            }

            Map<String, Object> fieldMap = (Map<String, Object>) value;

            // If this is the last segment, return the field properties
            if (i == fieldPath.size() - 1) {
                return fieldMap;
            }

            // Otherwise, navigate to the nested properties
            if (fieldMap.containsKey("properties")) {
                current = (Map<String, Object>) fieldMap.get("properties");
            }
            else {
                return null;
            }
        }

        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        OpenSearchColumnHandle column = (OpenSearchColumnHandle) columnHandle;
        return column.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        String schemaName = prefix.getSchemaName();
        if (schemaName != null && !DEFAULT_SCHEMA.equals(schemaName)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tables;
        String tableName = prefix.getTableName();
        if (tableName != null) {
            // Specific table requested
            tables = ImmutableList.of(new SchemaTableName(DEFAULT_SCHEMA, tableName));
        }
        else {
            // All tables
            tables = listTables(session, Optional.of(DEFAULT_SCHEMA));
        }

        for (SchemaTableName table : tables) {
            try {
                List<ColumnMetadata> tableColumns = getColumnsFromMapping(table.getTableName());
                columns.put(table, tableColumns);
            }
            catch (Exception e) {
                log.warn(e, "Failed to get columns for table: %s", table);
            }
        }

        return columns.build();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        OpenSearchTableHandle tableHandle = (OpenSearchTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new OpenSearchTableLayoutHandle(tableHandle),
                Optional.empty(),
                constraint.getSummary(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        OpenSearchTableLayoutHandle layoutHandle = (OpenSearchTableLayoutHandle) handle;
        return new ConnectorTableLayout(
                layoutHandle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    private List<ColumnMetadata> getColumnsFromMapping(String indexName)
    {
        Map<String, Object> mapping = client.getIndexMapping(indexName);
        List<ColumnMetadata> columns = new ArrayList<>();

        // Add _id column first unless configured to hide it
        if (!config.isHideDocumentIdColumn()) {
            columns.add(ColumnMetadata.builder()
                    .setName("_id")
                    .setType(VARCHAR)
                    .setComment("")
                    .build());
        }

        // Try to get stored column metadata (order and types)
        // OpenSearch returns fields alphabetically and loses VARCHAR length info
        Map<String, List<String>> storedMetadata = client.getColumnMetadata(indexName);
        List<String> columnOrder = storedMetadata.getOrDefault("column_order", new ArrayList<>());
        List<String> columnTypes = storedMetadata.getOrDefault("column_types", new ArrayList<>());

        // Build a map of column name to Presto type from stored metadata
        Map<String, Type> storedTypes = new HashMap<>();
        if (!columnOrder.isEmpty() && columnOrder.size() == columnTypes.size()) {
            for (int i = 0; i < columnOrder.size(); i++) {
                Type type = parsePrestoType(columnTypes.get(i));
                if (type != null) {
                    storedTypes.put(columnOrder.get(i), type);
                }
            }
        }

        // Check if nested field access is enabled
        if (config.isNestedFieldAccessEnabled()) {
            // Use NestedFieldMapper to discover all fields including nested ones
            Map<String, NestedFieldInfo> allFields = nestedFieldMapper.discoverNestedFields(mapping);

            // If we have stored column order, use it; otherwise use discovery order
            if (!columnOrder.isEmpty()) {
                // Add columns in the stored order
                for (String columnName : columnOrder) {
                    NestedFieldInfo fieldInfo = allFields.get(columnName);
                    if (fieldInfo != null) {
                        // Use stored type if available, otherwise infer
                        Type columnType;
                        if (storedTypes.containsKey(columnName)) {
                            columnType = storedTypes.get(columnName);
                        }
                        else {
                            columnType = fieldInfo.isLeafField() ? fieldInfo.getPrestoType() : VARCHAR;
                        }
                        columns.add(ColumnMetadata.builder()
                                .setName(fieldInfo.getFieldPath())
                                .setType(columnType)
                                .setComment("")
                                .build());
                    }
                }
            }
            else {
                // Fall back to discovery order
                for (Map.Entry<String, NestedFieldInfo> entry : allFields.entrySet()) {
                    NestedFieldInfo fieldInfo = entry.getValue();
                    // Use the Presto type from NestedFieldInfo (ROW for objects, primitive for leaves)
                    Type columnType = fieldInfo.getPrestoType();
                    columns.add(ColumnMetadata.builder()
                            .setName(fieldInfo.getFieldPath())
                            .setType(columnType)
                            .setComment("")
                            .build());
                }
            }
        }
        else {
            // Original flat field logic (backward compatibility)
            if (!columnOrder.isEmpty()) {
                // Add columns in the stored order
                for (String columnName : columnOrder) {
                    Map<String, Object> fieldProperties = (Map<String, Object>) mapping.get(columnName);
                    if (fieldProperties != null) {
                        // Use stored type if available, otherwise infer from OpenSearch type
                        Type prestoType;
                        if (storedTypes.containsKey(columnName)) {
                            prestoType = storedTypes.get(columnName);
                        }
                        else {
                            String fieldType = (String) fieldProperties.getOrDefault("type", "text");
                            prestoType = TypeMapper.toPrestoType(fieldType, fieldProperties, columnName);
                        }
                        columns.add(ColumnMetadata.builder()
                                .setName(columnName)
                                .setType(prestoType)
                                .setComment("")
                                .build());
                    }
                }
            }
            else {
                // Fall back to mapping order (alphabetical from OpenSearch)
                for (Map.Entry<String, Object> entry : mapping.entrySet()) {
                    String fieldName = entry.getKey();
                    Map<String, Object> fieldProperties = (Map<String, Object>) entry.getValue();
                    String fieldType = (String) fieldProperties.getOrDefault("type", "text");
                    Type prestoType = TypeMapper.toPrestoType(fieldType, fieldProperties, fieldName);
                    columns.add(ColumnMetadata.builder()
                            .setName(fieldName)
                            .setType(prestoType)
                            .setComment("")
                            .build());
                }
            }
        }

        log.debug("Retrieved %d columns from mapping for index %s", columns.size(), indexName);
        return columns;
    }

    /**
     * Parse a Presto type string (e.g., "varchar(25)", "bigint") into a Type object.
     */
    private Type parsePrestoType(String typeString)
    {
        if (typeString == null || typeString.isEmpty()) {
            return null;
        }

        // Handle parameterized types like varchar(25)
        if (typeString.startsWith("varchar(") && typeString.endsWith(")")) {
            try {
                String lengthStr = typeString.substring(8, typeString.length() - 1);
                int length = Integer.parseInt(lengthStr);
                return createVarcharType(length);
            }
            catch (NumberFormatException e) {
                log.warn("Failed to parse varchar length from: %s", typeString);
                return VARCHAR;
            }
        }

        // Handle simple types
        switch (typeString.toLowerCase(java.util.Locale.ENGLISH)) {
            case "varchar":
                return VARCHAR;
            case "bigint":
                return BIGINT;
            case "integer":
                return INTEGER;
            case "smallint":
                return SMALLINT;
            case "tinyint":
                return TINYINT;
            case "double":
                return DOUBLE;
            case "real":
                return REAL;
            case "boolean":
                return BOOLEAN;
            case "date":
                return DATE;
            case "timestamp":
                return TIMESTAMP;
            case "varbinary":
                return VARBINARY;
            default:
                log.warn("Unknown type string: %s, defaulting to VARCHAR", typeString);
                return VARCHAR;
        }
    }
}
