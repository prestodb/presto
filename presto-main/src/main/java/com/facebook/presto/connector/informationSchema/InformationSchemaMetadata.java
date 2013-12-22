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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
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
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
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
                    .column("table_catalog", STRING)
                    .column("table_schema", STRING)
                    .column("table_name", STRING)
                    .column("column_name", STRING)
                    .column("ordinal_position", LONG)
                    .column("column_default", STRING)
                    .column("is_nullable", STRING)
                    .column("data_type", STRING)
                    .column("is_partition_key", STRING)
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLES)
                    .column("table_catalog", STRING)
                    .column("table_schema", STRING)
                    .column("table_name", STRING)
                    .column("table_type", STRING)
                    .build())
            .table(tableMetadataBuilder(TABLE_SCHEMATA)
                    .column("catalog_name", STRING)
                    .column("schema_name", STRING)
                    .build())
            .table(tableMetadataBuilder(TABLE_INTERNAL_FUNCTIONS)
                    .column("function_name", STRING)
                    .column("argument_types", STRING)
                    .column("return_type", STRING)
                    .column("function_type", STRING)
                    .column("description", STRING)
                    .build())
            .table(tableMetadataBuilder(TABLE_INTERNAL_PARTITIONS)
                    .column("table_catalog", STRING)
                    .column("table_schema", STRING)
                    .column("table_name", STRING)
                    .column("partition_number", LONG)
                    .column("partition_key", STRING)
                    .column("partition_value", STRING)
                    .build())
            .build();

    private final String catalogName;

    public InformationSchemaMetadata(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        if (!(tableHandle instanceof InformationSchemaTableHandle)) {
            return false;
        }

        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) tableHandle;
        return handle.getCatalogName().equals(catalogName) && TABLES.containsKey(handle.getSchemaTableName());
    }

    private InformationSchemaTableHandle checkTableHandle(TableHandle tableHandle)
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
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        if (!TABLES.containsKey(tableName)) {
            return null;
        }
        return new InformationSchemaTableHandle(catalogName, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
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
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        if (findColumnMetadata(tableMetadata, columnName) == null) {
            return null;
        }
        return new InformationSchemaColumnHandle(columnName);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
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
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
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
