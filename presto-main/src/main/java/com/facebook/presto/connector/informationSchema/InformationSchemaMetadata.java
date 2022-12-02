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

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.EquatableValueSet;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTablePrefix;
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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.metadata.MetadataUtil.SchemaMetadataBuilder.schemaMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.findColumnMetadata;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Locale.ENGLISH;
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
                    .column("view_owner", createUnboundedVarcharType())
                    .column("view_definition", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_SCHEMATA)
                    .column("catalog_name", createUnboundedVarcharType())
                    .column("schema_name", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLE_PRIVILEGES)
                    .column("grantor", createUnboundedVarcharType())
                    .column("grantor_type", createUnboundedVarcharType())
                    .column("grantee", createUnboundedVarcharType())
                    .column("grantee_type", createUnboundedVarcharType())
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("privilege_type", createUnboundedVarcharType())
                    .column("is_grantable", createUnboundedVarcharType())
                    .column("with_hierarchy", createUnboundedVarcharType())
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

    private static final InformationSchemaColumnHandle CATALOG_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_catalog");
    private static final InformationSchemaColumnHandle SCHEMA_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_schema");
    private static final InformationSchemaColumnHandle TABLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_name");
    private static final int MAX_PREFIXES_COUNT = 100;

    private final String catalogName;
    private final Metadata metadata;

    public InformationSchemaMetadata(String catalogName, Metadata metadata)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
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

        return TABLES.keySet().stream()
                .filter(compose(schemaNameOrNull::equals, SchemaTableName::getSchemaName))
                .collect(toImmutableList());
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
        if (constraint.getSummary().isNone()) {
            return ImmutableList.of();
        }

        InformationSchemaTableHandle handle = checkTableHandle(table);

        Set<QualifiedTablePrefix> prefixes = calculatePrefixesWithSchemaName(session, constraint.getSummary(), constraint.predicate());
        if (isTablesEnumeratingTable(handle.getSchemaTableName())) {
            Set<QualifiedTablePrefix> tablePrefixes = calculatePrefixesWithTableName(session, prefixes, constraint.getSummary(), constraint.predicate());
            // in case of high number of prefixes it is better to populate all data and then filter
            if (tablePrefixes.size() <= MAX_PREFIXES_COUNT) {
                prefixes = tablePrefixes;
            }
        }
        if (prefixes.size() > MAX_PREFIXES_COUNT) {
            // in case of high number of prefixes it is better to populate all data and then filter
            prefixes = ImmutableSet.of(new QualifiedTablePrefix(catalogName));
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(new InformationSchemaTableLayoutHandle(handle, prefixes));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    private boolean isTablesEnumeratingTable(SchemaTableName schemaTableName)
    {
        return ImmutableSet.of(TABLE_COLUMNS, TABLE_VIEWS, TABLE_TABLES, TABLE_TABLES, TABLE_TABLE_PRIVILEGES).contains(schemaTableName);
    }

    private Set<QualifiedTablePrefix> calculatePrefixesWithSchemaName(
            ConnectorSession connectorSession,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        Optional<Set<String>> schemas = filterString(constraint, SCHEMA_COLUMN_HANDLE);
        if (schemas.isPresent()) {
            return schemas.get().stream()
                    .filter(this::isLowerCase)
                    .map(schema -> new QualifiedTablePrefix(catalogName, schema))
                    .collect(toImmutableSet());
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        return metadata.listSchemaNames(session, catalogName).stream()
                .filter(schema -> !predicate.isPresent() || predicate.get().test(schemaAsFixedValues(schema)))
                .map(schema -> new QualifiedTablePrefix(catalogName, schema))
                .collect(toImmutableSet());
    }

    public Set<QualifiedTablePrefix> calculatePrefixesWithTableName(
            ConnectorSession connectorSession,
            Set<QualifiedTablePrefix> prefixes,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();

        Optional<Set<String>> tables = filterString(constraint, TABLE_NAME_COLUMN_HANDLE);
        if (tables.isPresent()) {
            return prefixes.stream()
                    .flatMap(prefix -> tables.get().stream()
                            .filter(this::isLowerCase)
                            .map(table -> table.toLowerCase(ENGLISH))
                            .map(table -> new QualifiedObjectName(catalogName, prefix.getSchemaName().get(), table)))
                    .filter(objectName -> metadata.getTableHandle(session, objectName).isPresent() || metadata.getView(session, objectName).isPresent())
                    .map(QualifiedTablePrefix::toQualifiedTablePrefix)
                    .collect(toImmutableSet());
        }

        return prefixes.stream()
                .flatMap(prefix -> Stream.concat(
                        metadata.listTables(session, prefix).stream(),
                        metadata.listViews(session, prefix).stream()))
                .filter(objectName -> !predicate.isPresent() || predicate.get().test(asFixedValues(objectName)))
                .map(QualifiedTablePrefix::toQualifiedTablePrefix)
                .collect(toImmutableSet());
    }

    private <T> Optional<Set<String>> filterString(TupleDomain<T> constraint, T column)
    {
        if (constraint.isNone()) {
            return Optional.of(ImmutableSet.of());
        }

        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return Optional.empty();
        }

        if (domain.isSingleValue()) {
            return Optional.of(ImmutableSet.of(((Slice) domain.getSingleValue()).toStringUtf8()));
        }
        else if (domain.getValues() instanceof EquatableValueSet) {
            Collection<Object> values = ((EquatableValueSet) domain.getValues()).getValues();
            return Optional.of(values.stream()
                    .map(Slice.class::cast)
                    .map(Slice::toStringUtf8)
                    .collect(toImmutableSet()));
        }
        return Optional.empty();
    }

    private Map<ColumnHandle, NullableValue> schemaAsFixedValues(String schema)
    {
        return ImmutableMap.of(SCHEMA_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<ColumnHandle, NullableValue> asFixedValues(QualifiedObjectName objectName)
    {
        return ImmutableMap.of(
                CATALOG_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getCatalogName())),
                SCHEMA_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getSchemaName())),
                TABLE_NAME_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getObjectName())));
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

    private boolean isLowerCase(String value)
    {
        return value.toLowerCase(ENGLISH).equals(value);
    }
}
