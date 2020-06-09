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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.elasticsearch.client.ElasticsearchClient;
import com.facebook.presto.elasticsearch.client.IndexMetadata;
import com.facebook.presto.elasticsearch.client.IndexMetadata.DateTimeType;
import com.facebook.presto.elasticsearch.client.IndexMetadata.ObjectType;
import com.facebook.presto.elasticsearch.client.IndexMetadata.PrimitiveType;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.RowType.Field;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private static final String ORIGINAL_NAME = "original-name";

    public static final String SUPPORTS_PREDICATES = "supports-predicates";

    private final ElasticsearchClient client;
    private final String schemaName;

    @Inject
    public ElasticsearchMetadata(ElasticsearchClient client, ElasticsearchConfig config)
    {
        requireNonNull(config, "config is null");
        this.client = requireNonNull(client, "client is null");
        this.schemaName = config.getDefaultSchema();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(schemaName);
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.getSchemaName().equals(schemaName)) {
            String[] parts = tableName.getTableName().split(":", 2);
            String table = parts[0];
            Optional<String> query = Optional.empty();
            if (parts.length == 2) {
                query = Optional.of(parts[1]);
            }

            if (listTables(session, Optional.of(schemaName)).contains(new SchemaTableName(schemaName, table))) {
                return new ElasticsearchTableHandle(schemaName, table, query);
            }
        }
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        return getTableMetadata(handle.getSchema(), handle.getIndex());
    }

    private ConnectorTableMetadata getTableMetadata(String schemaName, String tableName)
    {
        IndexMetadata metadata = client.getIndexMetadata(tableName);

        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                toColumnMetadata(metadata));
    }

    private List<ColumnMetadata> toColumnMetadata(IndexMetadata metadata)
    {
        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();

        result.add(BuiltinColumns.ID.getMetadata());
        result.add(BuiltinColumns.SOURCE.getMetadata());
        result.add(BuiltinColumns.SCORE.getMetadata());

        for (IndexMetadata.Field field : metadata.getSchema().getFields()) {
            Type type = toPrestoType(field);
            if (type == null) {
                continue;
            }

            result.add(makeColumnMetadata(field.getName(), type, supportsPredicates(field.getType())));
        }

        return result.build();
    }

    private static boolean supportsPredicates(IndexMetadata.Type type)
    {
        if (type instanceof DateTimeType) {
            return true;
        }

        if (type instanceof PrimitiveType) {
            switch (((PrimitiveType) type).getName().toLowerCase(ENGLISH)) {
                case "boolean":
                case "byte":
                case "short":
                case "integer":
                case "long":
                case "double":
                case "float":
                case "keyword":
                    return true;
            }
        }

        return false;
    }

    private Type toPrestoType(IndexMetadata.Field metaDataField)
    {
        return toPrestoType(metaDataField, metaDataField.isArray());
    }

    private Type toPrestoType(IndexMetadata.Field metaDataField, boolean isArray)
    {
        IndexMetadata.Type type = metaDataField.getType();
        if (isArray) {
            Type elementType = toPrestoType(metaDataField, false);
            return new ArrayType(elementType);
        }
        else if (type instanceof PrimitiveType) {
            switch (((PrimitiveType) type).getName()) {
                case "float":
                    return REAL;
                case "double":
                    return DOUBLE;
                case "byte":
                    return TINYINT;
                case "short":
                    return SMALLINT;
                case "integer":
                    return INTEGER;
                case "long":
                    return BIGINT;
                case "string":
                case "text":
                case "keyword":
                    return VARCHAR;
                case "boolean":
                    return BOOLEAN;
                case "binary":
                    return VARBINARY;
            }
        }
        else if (type instanceof DateTimeType) {
            if (((DateTimeType) type).getFormats().isEmpty()) {
                return TIMESTAMP;
            }
            // otherwise, skip -- we don't support custom formats, yet
        }
        else if (type instanceof ObjectType) {
            ObjectType objectType = (ObjectType) type;

            List<Field> fields = objectType.getFields().stream()
                    .map(field -> RowType.field(field.getName(), toPrestoType(field)))
                    .collect(toImmutableList());

            return RowType.from(fields);
        }

        return null;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(this.schemaName)) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<SchemaTableName> result = ImmutableList.builder();

        client.getIndexes().stream()
                .map(index -> new SchemaTableName(this.schemaName, index))
                .forEach(result::add);

        client.getAliases().stream()
                .map(index -> new SchemaTableName(this.schemaName, index))
                .forEach(result::add);

        return result.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> results = ImmutableMap.builder();

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            results.put(column.getName(), new ElasticsearchColumnHandle(
                    (String) column.getProperties().getOrDefault(ORIGINAL_NAME, column.getName()),
                    column.getType(),
                    (Boolean) column.getProperties().get(SUPPORTS_PREDICATES)));
        }

        return results.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ElasticsearchColumnHandle handle = (ElasticsearchColumnHandle) columnHandle;
        return makeColumnMetadata(handle.getName(), handle.getType(), handle.isSupportsPredicates());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() != null && !prefix.getSchemaName().equals(schemaName)) {
            return ImmutableMap.of();
        }

        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            ConnectorTableMetadata metadata = getTableMetadata(prefix.getSchemaName(), prefix.getTableName());
            return ImmutableMap.of(metadata.getTable(), metadata.getColumns());
        }

        return listTables(session, prefix.getSchemaName()).stream()
                .map(name -> getTableMetadata(name.getSchemaName(), name.getTableName()))
                .collect(toImmutableMap(ConnectorTableMetadata::getTable, ConnectorTableMetadata::getColumns));
    }

    private static ColumnMetadata makeColumnMetadata(String name, Type type, boolean supportsPredicates)
    {
        return new ColumnMetadata(
                name,
                type,
                null,
                null,
                false,
                ImmutableMap.of(
                        ORIGINAL_NAME, name,
                        SUPPORTS_PREDICATES, supportsPredicates));
    }
}
