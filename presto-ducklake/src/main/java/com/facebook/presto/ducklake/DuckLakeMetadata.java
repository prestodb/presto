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
package com.facebook.presto.ducklake;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeSchema;
import com.facebook.presto.ducklake.catalog.DuckLakeSnapshot;
import com.facebook.presto.ducklake.catalog.DuckLakeTable;
import com.facebook.presto.ducklake.statistics.TableStatisticsMaker;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.ducklake.DuckLakeColumnHandle.PATH_COLUMN_HANDLE;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.PATH_COLUMN_METADATA;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.ROW_ID_COLUMN_HANDLE;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.ROW_ID_COLUMN_METADATA;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.ROW_POSITION_COLUMN_METADATA;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_SNAPSHOT;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static com.facebook.presto.ducklake.DuckLakeTableType.DATA;
import static com.facebook.presto.ducklake.DuckLakeTableType.SNAPSHOTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Coordinator-side metadata for one DuckLake transaction. Modeled on {@code
 * IcebergAbstractMetadata}: one instance is created per transaction by {@link
 * DuckLakeMetadataFactory} and it keeps a snapshot-scoped cache of catalog lookups, so a query
 * sees one consistent snapshot even though {@link DuckLakeCatalog} rows for a given snapshot are
 * immutable and safe to cache. Table handles carry the resolved snapshot and the built schema
 * (see {@link DuckLakeTableHandle}), so this class is the only place that resolves snapshots and
 * converts DuckLake types.
 */
public class DuckLakeMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(DuckLakeMetadata.class);

    private final DuckLakeCatalog catalog;
    private final TypeManager typeManager;
    private final TableStatisticsMaker tableStatisticsMaker;

    private final AtomicReference<Long> latestSnapshotId = new AtomicReference<>();
    private final ConcurrentHashMap<Long, List<DuckLakeSchema>> schemasCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, List<DuckLakeTable>>> tablesCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, PrestoDuckLakeSchema>> schemasByTableCache = new ConcurrentHashMap<>();

    public DuckLakeMetadata(DuckLakeCatalog catalog, TypeManager typeManager, TableStatisticsMaker tableStatisticsMaker)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableStatisticsMaker = requireNonNull(tableStatisticsMaker, "tableStatisticsMaker is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemas(getLatestSnapshotId()).stream()
                .map(DuckLakeSchema::getSchemaName)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(session, tableName, Optional.empty());
    }

    /**
     * Unlike the unversioned overload, a missing schema or table here must throw rather than
     * return {@code null}: the engine's {@code MetadataUtil.getOptionalTableHandle()} maps a
     * {@code null} connector result through {@code Optional.map(...).orElseGet(...)}, which
     * treats it the same as an absent {@link Optional} and silently retries the lookup WITHOUT
     * the version, so a {@code null} here would make a perfectly valid but too-early version
     * resolve to the latest snapshot instead of failing. DuckLake's snapshot ids are catalog-wide
     * (unlike Iceberg's per-table ones), so "this schema/table was not created yet as of a valid
     * snapshot" is a common, expected case that must fail loudly instead.
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        DuckLakeTableName name = DuckLakeTableName.from(tableName.getTableName());
        if (name.getTableType() != DATA) {
            // Pseudo-tables (for example $snapshots) are served by getSystemTable, not as a data table.
            return null;
        }

        long snapshotId = tableVersion.map(this::resolveSnapshotId).orElseGet(this::getLatestSnapshotId);

        Optional<DuckLakeSchema> schema = findSchema(snapshotId, tableName.getSchemaName());
        if (!schema.isPresent()) {
            if (tableVersion.isPresent()) {
                throw new TableNotFoundException(tableName, format("Table %s does not exist at DuckLake snapshot %s", tableName, snapshotId));
            }
            return null;
        }
        Optional<DuckLakeTable> table = findTable(snapshotId, schema.get(), name.getTableName());
        if (!table.isPresent()) {
            if (tableVersion.isPresent()) {
                throw new TableNotFoundException(tableName, format("Table %s does not exist at DuckLake snapshot %s", tableName, snapshotId));
            }
            return null;
        }

        PrestoDuckLakeSchema schemaAtSnapshot = buildSchema(snapshotId, table.get());
        return new DuckLakeTableHandle(
                tableName.getSchemaName(),
                name.withSnapshotId(snapshotId, tableVersion.isPresent()),
                table.get().getTableId(),
                table.get().getSchemaId(),
                table.get().getPath(),
                schemaAtSnapshot);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = getLatestSnapshotId();
        List<DuckLakeSchema> schemas;
        if (schemaName.isPresent()) {
            schemas = findSchema(snapshotId, schemaName.get()).map(ImmutableList::of).orElseGet(ImmutableList::of);
        }
        else {
            schemas = listSchemas(snapshotId);
        }

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (DuckLakeSchema schema : schemas) {
            for (DuckLakeTable table : listTablesOf(snapshotId, schema)) {
                tables.add(new SchemaTableName(schema.getSchemaName(), table.getTableName()));
            }
        }
        return tables.build();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;
        return new ConnectorTableMetadata(handle.getSchemaTableName(), getColumnsMetadata(handle));
    }

    private List<ColumnMetadata> getColumnsMetadata(DuckLakeTableHandle handle)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (DuckLakeColumnIdentity column : handle.getSchema().getColumns()) {
            columns.add(ColumnMetadata.builder()
                    .setName(column.getName())
                    .setType(TypeConverter.toPrestoType(column, typeManager))
                    .setNullable(true)
                    .build());
        }
        columns.add(PATH_COLUMN_METADATA);
        columns.add(ROW_ID_COLUMN_METADATA);
        columns.add(ROW_POSITION_COLUMN_METADATA);
        return columns.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) tableHandle;
        Map<Long, String> initialDefaults = handle.getSchema().getInitialDefaults();

        Map<String, ColumnHandle> columns = new LinkedHashMap<>();
        for (DuckLakeColumnIdentity column : handle.getSchema().getColumns()) {
            Optional<String> defaultValue = Optional.ofNullable(initialDefaults.get(column.getId()));
            columns.put(column.getName(), DuckLakeColumnHandle.create(column, typeManager, defaultValue));
        }
        columns.put(PATH_COLUMN_HANDLE.getName(), PATH_COLUMN_HANDLE);
        columns.put(ROW_ID_COLUMN_HANDLE.getName(), ROW_ID_COLUMN_HANDLE);
        columns.put(ROW_POSITION_COLUMN_HANDLE.getName(), ROW_POSITION_COLUMN_HANDLE);
        return columns;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DuckLakeColumnHandle column = (DuckLakeColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setHidden(column.isPathColumn() || column.isRowIdColumn() || column.isRowPositionColumn())
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null
                ? ImmutableList.of(prefix.toSchemaTableName())
                : listTables(session, Optional.ofNullable(prefix.getSchemaName()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            ConnectorTableHandle handle = getTableHandle(session, tableName);
            if (handle == null) {
                continue;
            }
            try {
                result.put(tableName, getTableMetadata(session, handle).getColumns());
            }
            catch (PrestoException e) {
                if (e.getErrorCode().equals(DUCKLAKE_UNSUPPORTED_TYPE.toErrorCode())) {
                    log.warn(e, "Skipping table %s: unsupported column type", tableName);
                    continue;
                }
                throw e;
            }
        }
        return result.build();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        DuckLakeTableHandle handle = (DuckLakeTableHandle) table;

        Map<String, DuckLakeColumnHandle> predicateColumns = constraint.getSummary().getDomains()
                .map(domains -> domains.keySet().stream()
                        .map(DuckLakeColumnHandle.class::cast)
                        .collect(toImmutableMap(DuckLakeColumnHandle::getName, identity())))
                .orElseGet(ImmutableMap::of);
        Optional<Set<DuckLakeColumnHandle>> requestedColumns = desiredColumns.map(columns -> columns.stream()
                .map(DuckLakeColumnHandle.class::cast)
                .collect(toImmutableSet()));

        DuckLakeTableLayoutHandle layoutHandle = new DuckLakeTableLayoutHandle(handle, constraint.getSummary(), predicateColumns, requestedColumns);
        ConnectorTableLayout layout = new ConnectorTableLayout(layoutHandle);
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout((DuckLakeTableLayoutHandle) handle);
    }

    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Optional<ConnectorTableLayoutHandle> tableLayoutHandle,
            List<ColumnHandle> columnHandles,
            Constraint<ColumnHandle> constraint)
    {
        return tableStatisticsMaker.makeTableStatistics(
                (DuckLakeTableHandle) tableHandle,
                tableLayoutHandle.map(DuckLakeTableLayoutHandle.class::cast),
                columnHandles,
                constraint);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        DuckLakeTableName name = DuckLakeTableName.from(tableName.getTableName());
        if (name.getTableType() != SNAPSHOTS) {
            return Optional.empty();
        }

        long snapshotId = getLatestSnapshotId();
        Optional<DuckLakeSchema> schema = findSchema(snapshotId, tableName.getSchemaName());
        if (!schema.isPresent() || !findTable(snapshotId, schema.get(), name.getTableName()).isPresent()) {
            return Optional.empty();
        }

        SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
        return Optional.of(new SnapshotsTable(systemTableName, catalog));
    }

    // The DuckLake connector only supports reads in this version: every SPI entry point that would
    // mutate a schema, table, view, or materialized view -- or collect statistics via ANALYZE -- is
    // rejected here with one consistent error message, instead of being left to fail later (or, worse,
    // to silently no-op) inside catalog/data-file code that was never written to handle writes.

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        throw readOnlyError("CREATE SCHEMA");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw readOnlyError("DROP SCHEMA");
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        throw readOnlyError("RENAME SCHEMA");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw readOnlyError("CREATE TABLE");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        throw readOnlyError("CREATE TABLE");
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw readOnlyError("DROP TABLE");
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw readOnlyError("TRUNCATE TABLE");
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw readOnlyError("RENAME TABLE");
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> properties)
    {
        throw readOnlyError("SET TABLE PROPERTIES");
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw readOnlyError("ADD COLUMN");
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw readOnlyError("RENAME COLUMN");
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        throw readOnlyError("DROP COLUMN");
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // The three-argument overload of beginInsert() delegates to this one by default, so
        // overriding only this method covers both INSERT call paths.
        throw readOnlyError("INSERT");
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // getDeleteRowIdColumn() delegates to this deprecated method by default, and the engine
        // calls it while analyzing a DELETE statement, before beginDelete() would ever be reached.
        throw readOnlyError("DELETE");
    }

    @Override
    public ConnectorDeleteTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw readOnlyError("DELETE");
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        // getUpdateRowIdColumn() delegates to this deprecated method by default, and the engine
        // calls it while analyzing an UPDATE statement, before beginUpdate() would ever be reached.
        throw readOnlyError("UPDATE");
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw readOnlyError("UPDATE");
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        throw readOnlyError("CREATE VIEW");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw readOnlyError("DROP VIEW");
    }

    @Override
    public void createMaterializedView(ConnectorSession session, ConnectorTableMetadata viewMetadata, MaterializedViewDefinition viewDefinition, boolean ignoreExisting)
    {
        throw readOnlyError("CREATE MATERIALIZED VIEW");
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        // The engine resolves this method first while analyzing ANALYZE, before either
        // getStatisticsCollectionMetadata() or beginStatisticsCollection() is ever reached.
        throw readOnlyError("ANALYZE");
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw readOnlyError("ANALYZE");
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw readOnlyError("ANALYZE");
    }

    private static PrestoException readOnlyError(String operation)
    {
        return new PrestoException(NOT_SUPPORTED, "The DuckLake connector is read-only in this version: " + operation + " is not supported");
    }

    private long getLatestSnapshotId()
    {
        return latestSnapshotId.updateAndGet(existing -> existing != null ? existing : catalog.getLatestSnapshotId());
    }

    private List<DuckLakeSchema> listSchemas(long snapshotId)
    {
        return schemasCache.computeIfAbsent(snapshotId, catalog::listSchemas);
    }

    private Optional<DuckLakeSchema> findSchema(long snapshotId, String schemaName)
    {
        return listSchemas(snapshotId).stream()
                .filter(schema -> schema.getSchemaName().equals(schemaName))
                .findFirst();
    }

    private List<DuckLakeTable> listTablesOf(long snapshotId, DuckLakeSchema schema)
    {
        return tablesCache
                .computeIfAbsent(snapshotId, ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(schema.getSchemaId(), ignored -> catalog.listTables(snapshotId, schema));
    }

    private Optional<DuckLakeTable> findTable(long snapshotId, DuckLakeSchema schema, String tableName)
    {
        return listTablesOf(snapshotId, schema).stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst();
    }

    private PrestoDuckLakeSchema buildSchema(long snapshotId, DuckLakeTable table)
    {
        return schemasByTableCache
                .computeIfAbsent(snapshotId, ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(table.getTableId(), ignored -> SchemaBuilder.buildSchema(
                        catalog.listColumns(snapshotId, table.getTableId()),
                        catalog.listPartitionFields(snapshotId, table.getTableId())));
    }

    /**
     * Resolves a {@code FOR SYSTEM_VERSION}/{@code FOR SYSTEM_TIME AS OF/BEFORE} expression to a
     * snapshot id (spec &sect;4.2).
     */
    private long resolveSnapshotId(ConnectorTableVersion tableVersion)
    {
        switch (tableVersion.getVersionType()) {
            case VERSION:
                return resolveVersionSnapshotId(tableVersion);
            case TIMESTAMP:
                return resolveTimestampSnapshotId(tableVersion);
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported table version type: " + tableVersion.getVersionType());
    }

    private long resolveVersionSnapshotId(ConnectorTableVersion tableVersion)
    {
        if (!(tableVersion.getVersionExpressionType() instanceof BigintType)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported table version expression type: " + tableVersion.getVersionExpressionType());
        }
        long snapshotId = (long) tableVersion.getTableVersion();
        DuckLakeSnapshot snapshot = catalog.getSnapshot(snapshotId)
                .orElseThrow(() -> new PrestoException(DUCKLAKE_INVALID_SNAPSHOT, format("DuckLake snapshot %s does not exist", snapshotId)));

        if (tableVersion.getVersionOperator() == VersionOperator.EQUAL) {
            return snapshot.getSnapshotId();
        }
        // BEFORE: the greatest snapshot strictly earlier than the given one. PostgreSQL's timestamp
        // resolution is microseconds, so one microsecond earlier is exactly "strictly before".
        return catalog.getSnapshotAtOrBefore(snapshot.getSnapshotTime().minusNanos(1000))
                .orElseThrow(() -> new PrestoException(DUCKLAKE_INVALID_SNAPSHOT, format("No DuckLake snapshot exists before snapshot %s", snapshotId)))
                .getSnapshotId();
    }

    private long resolveTimestampSnapshotId(ConnectorTableVersion tableVersion)
    {
        Instant instant = toInstant(tableVersion);
        Instant lookupTime = tableVersion.getVersionOperator() == VersionOperator.LESS_THAN ? instant.minusNanos(1000) : instant;
        return catalog.getSnapshotAtOrBefore(lookupTime)
                .orElseThrow(() -> new PrestoException(DUCKLAKE_INVALID_SNAPSHOT, "No DuckLake snapshot exists at or before " + instant))
                .getSnapshotId();
    }

    private static Instant toInstant(ConnectorTableVersion tableVersion)
    {
        Type expressionType = tableVersion.getVersionExpressionType();
        long millisUtc;
        if (expressionType instanceof TimestampWithTimeZoneType) {
            millisUtc = new SqlTimestampWithTimeZone((long) tableVersion.getTableVersion()).getMillisUtc();
        }
        else if (expressionType instanceof TimestampType) {
            millisUtc = ((TimestampType) expressionType).toEpochMillis((long) tableVersion.getTableVersion());
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported table version expression type: " + expressionType);
        }
        return Instant.ofEpochMilli(millisUtc);
    }
}
