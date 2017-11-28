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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.SchemaMetadataBuilder.schemaMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.findColumnMetadata;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.filter;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class InformationSchemaMetadata
        implements ConnectorMetadata
{
    public static final String INFORMATION_SCHEMA = "information_schema";

    public static final SchemaTableName TABLE_COLUMNS = new SchemaTableName(INFORMATION_SCHEMA, "columns");
    public static final SchemaTableName TABLE_TABLES = new SchemaTableName(INFORMATION_SCHEMA, "tables");
    public static final SchemaTableName TABLE_VIEWS = new SchemaTableName(INFORMATION_SCHEMA, "views");
    public static final SchemaTableName TABLE_SCHEMATA = new SchemaTableName(INFORMATION_SCHEMA, "schemata");
    public static final SchemaTableName TABLE_INTERNAL_PARTITIONS = new SchemaTableName(INFORMATION_SCHEMA, "__internal_partitions__");
    public static final SchemaTableName TABLE_TABLE_PRIVILEGES = new SchemaTableName(INFORMATION_SCHEMA, "table_privileges");
    public static final SchemaTableName TABLE_ROLES = new SchemaTableName(INFORMATION_SCHEMA, "roles");
    public static final SchemaTableName TABLE_APPLICABLE_ROLES = new SchemaTableName(INFORMATION_SCHEMA, "applicable_roles");
    public static final SchemaTableName TABLE_ENABLED_ROLES = new SchemaTableName(INFORMATION_SCHEMA, "enabled_roles");

    public static final Map<SchemaTableName, ConnectorTableMetadata> TABLES = schemaMetadataBuilder()
            .table(tableMetadataBuilder(TABLE_COLUMNS)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("column_name", createUnboundedVarcharType())
                    .column("ordinal_position", BIGINT)
                    .column("column_default", createUnboundedVarcharType())
                    .column("is_nullable", createUnboundedVarcharType())
                    .column("data_type", createUnboundedVarcharType())
                    .column("comment", createUnboundedVarcharType())
                    .column("extra_info", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLES)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("table_type", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_VIEWS)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("view_definition", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_SCHEMATA)
                    .column("catalog_name", createUnboundedVarcharType())
                    .column("schema_name", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_INTERNAL_PARTITIONS)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("partition_number", BIGINT)
                    .column("partition_key", createUnboundedVarcharType())
                    .column("partition_value", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLE_PRIVILEGES)
                    .column("grantor", createUnboundedVarcharType())
                    .column("grantee", createUnboundedVarcharType())
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("privilege_type", createUnboundedVarcharType())
                    .column("is_grantable", BOOLEAN)
                    .column("with_hierarchy", BOOLEAN)
                    .build())
            .table(tableMetadataBuilder(TABLE_ROLES)
                    .column("role_name", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_APPLICABLE_ROLES)
                    .column("grantee", createUnboundedVarcharType())
                    .column("grantee_type", createUnboundedVarcharType())
                    .column("role_name", createUnboundedVarcharType())
                    .column("is_grantable", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_ENABLED_ROLES)
                    .column("role_name", createUnboundedVarcharType())
                    .build())
            .build();

    private final String catalogName;

    public InformationSchemaMetadata(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    private InformationSchemaTableHandle checkTableHandle(ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) tableHandle;
        checkArgument(handle.getCatalogName().equals(catalogName), "invalid table handle: expected catalog %s but got %s", catalogName, handle.getCatalogName());
        checkArgument(TABLES.containsKey(handle.getSchemaTableName()), "table %s does not exist", handle.getSchemaTableName());
        return handle;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(INFORMATION_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName tableName)
    {
        if (!TABLES.containsKey(tableName)) {
            return null;
        }

        return new InformationSchemaTableHandle(catalogName, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        return TABLES.get(informationSchemaTableHandle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return ImmutableList.copyOf(TABLES.keySet());
        }

        return ImmutableList.copyOf(filter(TABLES.keySet(), compose(equalTo(schemaNameOrNull), SchemaTableName::getSchemaName)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        String columnName = ((InformationSchemaColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);

        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        return tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toMap(identity(), InformationSchemaColumnHandle::new));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (Entry<SchemaTableName, ConnectorTableMetadata> entry : TABLES.entrySet()) {
            if (prefix.matches(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue().getColumns());
            }
        }
        return builder.build();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new InformationSchemaTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    static List<ColumnMetadata> informationSchemaTableColumns(SchemaTableName tableName)
    {
        checkArgument(TABLES.containsKey(tableName), "table does not exist: %s", tableName);
        return TABLES.get(tableName).getColumns();
    }
}
