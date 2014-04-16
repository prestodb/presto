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
package com.facebook.presto.connector.jdbcSchema;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.connector.jdbcSchema.JdbcSchemaColumnHandle.toJdbcSchemaColumnHandles;
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

public class JdbcSchemaMetadata
        extends ReadOnlyConnectorMetadata
{
    Logger log = Logger.get(JdbcSchemaMetadata.class);

    public static final String JDBC_SCHEMA = "jdbc_schema";

    public static final SchemaTableName TABLE_COLUMNS = new SchemaTableName(JDBC_SCHEMA, "columns");
    public static final SchemaTableName TABLE_TABLES = new SchemaTableName(JDBC_SCHEMA, "tables");
    public static final SchemaTableName TABLE_FUNCTIONS = new SchemaTableName(JDBC_SCHEMA, "functions");

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
                    .column("table_cat", STRING)
                    .column("table_schem", STRING)
                    .column("table_name", STRING)
                    .column("table_type", STRING)
                    .column("remarks", STRING)
                    .column("type_cat", STRING)
                    .column("type_schem", STRING)
                    .column("type_name", STRING)
                    .column("self_referencing_col_name", STRING)
                    .column("ref_generation", STRING)
                    .build())
            .table(tableMetadataBuilder(TABLE_FUNCTIONS)
                    .column("function_cat", STRING)
                    .column("function_schema", STRING)
                    .column("function_name", STRING)
                    .column("remarks", STRING)
                    .column("function_type", LONG)
                    .column("specific_name", STRING)
                    .build())
            .build();

    private final String catalogName;

    public JdbcSchemaMetadata(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        if (!(tableHandle instanceof JdbcSchemaTableHandle)) {
            return false;
        }

        JdbcSchemaTableHandle handle = (JdbcSchemaTableHandle) tableHandle;
        return handle.getCatalogName().equals(catalogName) && TABLES.containsKey(handle.getSchemaTableName());
    }

    private JdbcSchemaTableHandle checkTableHandle(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof JdbcSchemaTableHandle, "tableHandle is not an jdbc schema table handle");

        JdbcSchemaTableHandle handle = (JdbcSchemaTableHandle) tableHandle;

        checkArgument(handle.getCatalogName().equals(catalogName), "invalid table handle: expected catalog %s but got %s", catalogName, handle.getCatalogName());
        checkArgument(TABLES.containsKey(handle.getSchemaTableName()), "table %s does not exist", handle.getSchemaTableName());
        return handle;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of(JDBC_SCHEMA);
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        if (!TABLES.containsKey(tableName)) {
            return null;
        }
        return new JdbcSchemaTableHandle(catalogName, tableName.getSchemaName(), tableName.getTableName());
        //return new JdbcSchemaTableHandle("toto", tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        JdbcSchemaTableHandle jdbcSchemaTableHandle = checkTableHandle(tableHandle);
        return TABLES.get(jdbcSchemaTableHandle.getSchemaTableName());
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
        JdbcSchemaTableHandle jdbcSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(jdbcSchemaTableHandle.getSchemaTableName());

        if (findColumnMetadata(tableMetadata, columnName) == null) {
            return null;
        }
        return new JdbcSchemaColumnHandle(columnName);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        JdbcSchemaTableHandle jdbcSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(jdbcSchemaTableHandle.getSchemaTableName());

        checkArgument(columnHandle instanceof JdbcSchemaColumnHandle, "columnHandle is not an instance of JdbcSchemaColumnHandle");
        String columnName = ((JdbcSchemaColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        JdbcSchemaTableHandle jdbcSchemaTableHandle = checkTableHandle(tableHandle);

        ConnectorTableMetadata tableMetadata = TABLES.get(jdbcSchemaTableHandle.getSchemaTableName());

        return toJdbcSchemaColumnHandles(tableMetadata);
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

    static List<ColumnMetadata> jdbcSchemaTableColumns(SchemaTableName tableName)
    {
        checkArgument(TABLES.containsKey(tableName), "table does not exist: %s", tableName);
        return TABLES.get(tableName).getColumns();
    }
}
