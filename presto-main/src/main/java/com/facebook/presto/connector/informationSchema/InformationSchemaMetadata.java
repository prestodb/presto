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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.connector.informationSchema.InformationSchemaColumnHandle.toInformationSchemaColumnHandles;
import static com.facebook.presto.metadata.MetadataUtil.SchemaMetadataBuilder.schemaMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.findColumnMetadata;
import static com.facebook.presto.metadata.MetadataUtil.schemaNameGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.filter;

public class InformationSchemaMetadata
        extends ReadOnlyConnectorMetadata
{
    public static final String INFORMATION_SCHEMA = "information_schema";

    public static final SchemaTableName TABLE_COLUMNS = new SchemaTableName(INFORMATION_SCHEMA, "columns");
    public static final SchemaTableName TABLE_TABLES = new SchemaTableName(INFORMATION_SCHEMA, "tables");
    public static final SchemaTableName TABLE_SCHEMATA = new SchemaTableName(INFORMATION_SCHEMA, "schemata");
    public static final SchemaTableName TABLE_INTERNAL_FUNCTIONS = new SchemaTableName(INFORMATION_SCHEMA, "__internal_functions__");
    public static final SchemaTableName TABLE_INTERNAL_PARTITIONS = new SchemaTableName(INFORMATION_SCHEMA, "__internal_partitions__");

    public static final Map<SchemaTableName, ConnectorTableMetadata> TABLES = schemaMetadataBuilder()
            .table(tableMetadataBuilder(TABLE_COLUMNS)
                    .column("table_catalog", VARCHAR)
                    .column("table_schema", VARCHAR)
                    .column("table_name", VARCHAR)
                    .column("column_name", VARCHAR)
                    .column("ordinal_position", BIGINT)
                    .column("column_default", VARCHAR)
                    .column("is_nullable", VARCHAR)
                    .column("data_type", VARCHAR)
                    .column("is_partition_key", VARCHAR)
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLES)
                    .column("table_catalog", VARCHAR)
                    .column("table_schema", VARCHAR)
                    .column("table_name", VARCHAR)
                    .column("table_type", VARCHAR)
                    .build())
            .table(tableMetadataBuilder(TABLE_SCHEMATA)
                    .column("catalog_name", VARCHAR)
                    .column("schema_name", VARCHAR)
                    .build())
            .table(tableMetadataBuilder(TABLE_INTERNAL_FUNCTIONS)
                    .column("function_name", VARCHAR)
                    .column("argument_types", VARCHAR)
                    .column("return_type", VARCHAR)
                    .column("function_type", VARCHAR)
                    .column("description", VARCHAR)
                    .build())
            .table(tableMetadataBuilder(TABLE_INTERNAL_PARTITIONS)
                    .column("table_catalog", VARCHAR)
                    .column("table_schema", VARCHAR)
                    .column("table_name", VARCHAR)
                    .column("partition_number", BIGINT)
                    .column("partition_key", VARCHAR)
                    .column("partition_value", VARCHAR)
                    .build())
            .build();

    private final String catalogName;

    public InformationSchemaMetadata(String catalogName)
    {
        this.catalogName = catalogName;
    }

    private InformationSchemaTableHandle checkTableHandle(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof InformationSchemaTableHandle, "tableHandle is not an information schema table handle");

        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) tableHandle;
        checkArgument(handle.getCatalogName().equals(catalogName), "invalid table handle: expected catalog %s but got %s", catalogName, handle.getCatalogName());
        checkArgument(TABLES.containsKey(handle.getSchemaTableName()), "table %s does not exist", handle.getSchemaTableName());
        return handle;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of(INFORMATION_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        if (!TABLES.containsKey(tableName)) {
            return null;
        }
        return new InformationSchemaTableHandle(catalogName, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        return TABLES.get(informationSchemaTableHandle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(final String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return ImmutableList.copyOf(TABLES.keySet());
        }

        return ImmutableList.copyOf(filter(TABLES.keySet(), compose(equalTo(schemaNameOrNull), schemaNameGetter())));
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        if (findColumnMetadata(tableMetadata, columnName) == null) {
            return null;
        }
        return new InformationSchemaColumnHandle(columnName);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        checkArgument(columnHandle instanceof InformationSchemaColumnHandle, "columnHandle is not an instance of InformationSchemaColumnHandle");
        String columnName = ((InformationSchemaColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);

        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        return toInformationSchemaColumnHandles(tableMetadata);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (Entry<SchemaTableName, ConnectorTableMetadata> entry : TABLES.entrySet()) {
            if (prefix.matches(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue().getColumns());
            }
        }
        return builder.build();
    }

    static List<ColumnMetadata> informationSchemaTableColumns(SchemaTableName tableName)
    {
        checkArgument(TABLES.containsKey(tableName), "table does not exist: %s", tableName);
        return TABLES.get(tableName).getColumns();
    }
}
