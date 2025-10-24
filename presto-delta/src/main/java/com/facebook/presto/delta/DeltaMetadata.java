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
package com.facebook.presto.delta;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.delta.DeltaExpressionUtils.splitPredicate;
import static com.facebook.presto.delta.DeltaTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static com.facebook.presto.delta.DeltaTableProperties.getTableStorageFormat;
import static com.facebook.presto.delta.DeltaTableProperties.isExternalTable;
import static com.facebook.presto.hive.HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;

public class DeltaMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(DeltaMetadata.class);

    /**
     * Special schema used when querying a Delta table by storage location.
     * Ex. SELECT * FROM delta."$PATH$"."s3://bucket/path/to/table". User is not able to list any tables
     * in this schema. It is just used to query a Delta table by storage location.
     */
    private static final String PATH_SCHEMA = "$PATH$";

    private final String connectorId;
    private final DeltaClient deltaClient;
    private final ExtendedHiveMetastore metastore;
    private final TypeManager typeManager;
    private final DeltaConfig config;

    @Inject
    public DeltaMetadata(
            DeltaConnectorId connectorId,
            DeltaClient deltaClient,
            ExtendedHiveMetastore metastore,
            TypeManager typeManager,
            DeltaConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.deltaClient = requireNonNull(deltaClient, "deltaClient is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ArrayList<String> schemas = new ArrayList<>();
        schemas.addAll(metastore.getAllDatabases(metastoreContext(session)));
        schemas.add(PATH_SCHEMA.toLowerCase(US));
        return schemas;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        PrestoTableType tableType = isExternalTable(tableMetadata.getProperties()) ? EXTERNAL_TABLE : MANAGED_TABLE;
        Table table = prepareTable(session, tableMetadata, tableType);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner());

        metastore.createTable(
                metastoreContext(session),
                table,
                principalPrivileges,
                emptyList());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DeltaTableHandle handle = (DeltaTableHandle) tableHandle;
        MetastoreContext metastoreContext = metastoreContext(session);

        Optional<Table> target = metastore.getTable(metastoreContext, handle.getDeltaTable().getSchemaName(), handle.getDeltaTable().getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(handle.toSchemaTableName());
        }

        metastore.dropTable(
                metastoreContext,
                handle.getDeltaTable().getSchemaName(),
                handle.getDeltaTable().getTableName(),
                false);
    }

    private static PrincipalPrivileges buildInitialPrivilegeSet(String tableOwner)
    {
        PrestoPrincipal owner = new PrestoPrincipal(USER, tableOwner);
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, true, owner, owner))
                        .build(),
                ImmutableMultimap.of());
    }

    private Table prepareTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, PrestoTableType tableType)
    {
        String schemaName = tableMetadata.getTable().getSchemaName();
        String tableName = tableMetadata.getTable().getTableName();

        if (!tableType.equals(EXTERNAL_TABLE)) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot create managed Delta table");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(session.getUser())
                .setTableType(tableType);

        Map<String, Object> tableProperties = tableMetadata.getProperties();

        HiveStorageFormat hiveStorageFormat = getTableStorageFormat(tableMetadata.getProperties());
        String tableLocation = tableProperties.get(EXTERNAL_LOCATION_PROPERTY).toString();

        tableBuilder.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(hiveStorageFormat))
                .setLocation(tableLocation);
        return tableBuilder.build();
    }

    @Override
    public DeltaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (!listSchemaNames(session).contains(schemaName)) {
            return null; // indicates table doesn't exist
        }

        DeltaTableName deltaTableName = DeltaTableName.from(tableName);
        String tableLocation;
        if (PATH_SCHEMA.equalsIgnoreCase(schemaName)) {
            tableLocation = deltaTableName.getTableNameOrPath();
        }
        else {
            Optional<Table> metastoreTable = metastore.getTable(metastoreContext(session), schemaName, deltaTableName.getTableNameOrPath());
            if (!metastoreTable.isPresent()) {
                return null; // indicates table doesn't exist
            }

            Map<String, String> tableParameters = metastoreTable.get().getParameters();
            Storage storage = metastoreTable.get().getStorage();
            tableLocation = storage.getLocation();

            // Delta table written using Spark and Hive have set the table parameter
            // "spark.sql.sources.provider = delta". If this property is found table
            // location is found in SerDe properties with key "path".
            if ("delta".equalsIgnoreCase(tableParameters.get("spark.sql.sources.provider"))) {
                tableLocation = storage.getSerdeParameters().get("path");
                if (Strings.isNullOrEmpty(tableLocation)) {
                    log.warn("Location key ('path') is missing in SerDe properties for table %s. " +
                            "Using the 'location' attribute as the table location.", schemaTableName);
                    // fallback to using the location attribute
                    tableLocation = storage.getLocation();
                }
            }
        }

        Optional<DeltaTable> table = deltaClient.getTable(
                config,
                session,
                schemaTableName,
                tableLocation,
                deltaTableName.getSnapshotId(),
                deltaTableName.getTimestampMillisUtc());
        if (table.isPresent()) {
            return new DeltaTableHandle(connectorId, table.get());
        }
        return null;
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        DeltaTableHandle tableHandle = (DeltaTableHandle) table;

        // Split the predicate into partition column predicate and other column predicates
        // Only the partition column predicate is fully enforced. Other predicate is partially enforced (best effort).
        List<TupleDomain<ColumnHandle>> predicate = splitPredicate(constraint.getSummary());
        TupleDomain<ColumnHandle> unenforcedPredicate = predicate.get(1);

        DeltaTableLayoutHandle newDeltaTableLayoutHandle = new DeltaTableLayoutHandle(
                tableHandle,
                constraint.getSummary().transform(DeltaColumnHandle.class::cast),
                Optional.of(constraint.getSummary().toString(session.getSqlFunctionProperties())));

        ConnectorTableLayout newLayout = new ConnectorTableLayout(
                newDeltaTableLayoutHandle,
                Optional.empty(),
                constraint.getSummary(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());

        return new ConnectorTableLayoutResult(newLayout, unenforcedPredicate);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DeltaTableHandle deltaTableHandle = (DeltaTableHandle) table;
        checkConnectorId(deltaTableHandle);
        return getTableMetadata(session, deltaTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<String> schemaNames = schemaName.<List<String>>map(ImmutableList::of)
                .orElseGet(() -> listSchemaNames(session));
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schema : schemaNames) {
            for (String tableName : metastore.getAllTables(metastoreContext(session), schema).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schema, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DeltaTableHandle deltaTableHandle = (DeltaTableHandle) tableHandle;
        checkConnectorId(deltaTableHandle);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DeltaColumn column : deltaTableHandle.getDeltaTable().getColumns()) {
            columnHandles.put(
                    column.getName(),
                    new DeltaColumnHandle(
                            column.getName(),
                            column.getType(),
                            column.isPartition() ? PARTITION : REGULAR,
                            Optional.empty()));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        DeltaTableHandle tableHandle = getTableHandle(session, tableName);
        if (tableHandle == null) {
            return null;
        }

        List<ColumnMetadata> columnMetadata = tableHandle.getDeltaTable().getColumns().stream()
                .map(column -> getColumnMetadata(session, column))
                .collect(Collectors.toList());

        return new ConnectorTableMetadata(tableName, columnMetadata);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return getColumnMetadata(columnHandle);
    }

    private ColumnMetadata getColumnMetadata(ColumnHandle columnHandle)
    {
        DeltaColumnHandle deltaColumnHandle = (DeltaColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(deltaColumnHandle.getName())
                .setType(typeManager.getType(deltaColumnHandle.getDataType()))
                .build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    private ColumnMetadata getColumnMetadata(ConnectorSession session, DeltaColumn deltaColumn)
    {
        return ColumnMetadata.builder()
                .setName(normalizeIdentifier(session, deltaColumn.getName()))
                .setType(typeManager.getType(deltaColumn.getType()))
                .build();
    }

    private MetastoreContext metastoreContext(ConnectorSession session)
    {
        return new MetastoreContext(
                session.getIdentity(),
                session.getQueryId(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getSource(),
                Optional.empty(),
                false,
                DEFAULT_COLUMN_CONVERTER_PROVIDER,
                session.getWarningCollector(),
                session.getRuntimeStats());
    }

    private void checkConnectorId(DeltaTableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorId().equals(connectorId), "table handle is not for this connector");
    }
}
