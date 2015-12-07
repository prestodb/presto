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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.ViewResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import static com.facebook.presto.raptor.RaptorColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorColumnHandle.SHARD_UUID_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardRowIdHandle;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardUuidColumnHandle;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorSessionProperties.getExternalBatchId;
import static com.facebook.presto.raptor.RaptorTableProperties.getSortColumns;
import static com.facebook.presto.raptor.RaptorTableProperties.getTemporalColumn;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final JsonCodec<ShardDelta> shardDeltaCodec;
    private final String connectorId;

    @Inject
    public RaptorMetadata(
            RaptorConnectorId connectorId,
            @ForMetadata IDBI dbi,
            ShardManager shardManager,
            JsonCodec<ShardInfo> shardInfoCodec,
            JsonCodec<ShardDelta> shardDeltaCodec)
    {
        requireNonNull(connectorId, "connectorId is null");

        this.connectorId = connectorId.toString();
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.shardInfoCodec = requireNonNull(shardInfoCodec, "shardInfoCodec is null");
        this.shardDeltaCodec = requireNonNull(shardDeltaCodec, "shardDeltaCodec is null");

        createTablesWithRetry(dbi);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return dao.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        List<TableColumn> tableColumns = dao.getTableColumns(table.getTableId());
        checkArgument(!tableColumns.isEmpty(), "Table %s does not have any columns", tableName);

        RaptorColumnHandle sampleWeightColumnHandle = null;
        for (TableColumn tableColumn : tableColumns) {
            if (SAMPLE_WEIGHT_COLUMN_NAME.equals(tableColumn.getColumnName())) {
                sampleWeightColumnHandle = getRaptorColumnHandle(tableColumn);
            }
        }

        return new RaptorTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTableId(),
                OptionalLong.empty(),
                Optional.ofNullable(sampleWeightColumnHandle));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        List<ColumnMetadata> columns = dao.getTableColumns(handle.getTableId()).stream()
                .map(TableColumn::toColumnMetadata)
                .filter(isSampleWeightColumn().negate())
                .collect(toCollection(ArrayList::new));
        if (columns.isEmpty()) {
            throw new PrestoException(RAPTOR_ERROR, "Table does not have any columns: " + tableName);
        }

        columns.add(hiddenColumn(SHARD_UUID_COLUMN_NAME, VARCHAR));
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return dao.listTables(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(raptorTableHandle.getTableId())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            builder.put(tableColumn.getColumnName(), getRaptorColumnHandle(tableColumn));
        }
        RaptorColumnHandle uuidColumn = shardUuidColumnHandle(connectorId);
        builder.put(uuidColumn.getColumnName(), uuidColumn);
        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getSampleWeightColumnHandle().orElse(null);
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();
        RaptorColumnHandle column = checkType(columnHandle, RaptorColumnHandle.class, "columnHandle");

        if (column.isShardRowId() || column.isShardUuid()) {
            return hiddenColumn(column.getColumnName(), column.getColumnType());
        }

        long columnId = column.getColumnId();
        TableColumn tableColumn = dao.getTableColumn(tableId, columnId);
        if (tableColumn == null) {
            throw new PrestoException(NOT_FOUND, format("Column ID %s does not exist for table ID %s", columnId, tableId));
        }
        return tableColumn.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(prefix.getSchemaName(), prefix.getTableName())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType(), false);
            columns.put(tableColumn.getTable(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        RaptorTableHandle handle = checkType(table, RaptorTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new RaptorTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        commitCreateTable(session, beginCreateTable(session, tableMetadata), ImmutableList.of());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        shardManager.dropTable(raptorHandle.getTableId());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        runTransaction(dbi, (handle, status) -> {
            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.renameTable(table.getTableId(), newTableName.getSchemaName(), newTableName.getTableName());
            return null;
        });
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");

        // Always add new columns to the end.
        // TODO: This needs to be updated when we support dropping columns.
        List<TableColumn> existingColumns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        TableColumn lastColumn = existingColumns.get(existingColumns.size() - 1);
        long columnId = lastColumn.getColumnId() + 1;
        int ordinalPosition = existingColumns.size();

        String type = column.getType().getTypeSignature().toString();
        dao.insertColumn(table.getTableId(), columnId, column.getName(), ordinalPosition, type, null);
        shardManager.addColumn(table.getTableId(), new ColumnInfo(columnId, column.getType()));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        RaptorColumnHandle sourceColumn = checkType(source, RaptorColumnHandle.class, "columnHandle");
        dao.renameColumn(table.getTableId(), sourceColumn.getColumnId(), target);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        long columnId = 1;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnTypes.add(column.getType());
            columnId++;
        }
        Map<String, RaptorColumnHandle> columnHandleMap = Maps.uniqueIndex(columnHandles.build(), RaptorColumnHandle::getColumnName);

        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(getSortColumns(tableMetadata.getProperties()), columnHandleMap);
        Optional<RaptorColumnHandle> temporalColumnHandle = getTemporalColumnHandle(getTemporalColumn(tableMetadata.getProperties()), columnHandleMap);

        if (temporalColumnHandle.isPresent()) {
            RaptorColumnHandle column = temporalColumnHandle.get();
            if (!column.getColumnType().equals(TIMESTAMP) && !column.getColumnType().equals(DATE)) {
                throw new PrestoException(NOT_SUPPORTED, "Temporal column must be of type timestamp or date: " + column.getColumnName());
            }
        }

        RaptorColumnHandle sampleWeightColumnHandle = null;
        if (tableMetadata.isSampled()) {
            sampleWeightColumnHandle = new RaptorColumnHandle(connectorId, SAMPLE_WEIGHT_COLUMN_NAME, columnId, BIGINT);
            columnHandles.add(sampleWeightColumnHandle);
            columnTypes.add(BIGINT);
        }

        long transactionId = shardManager.beginTransaction();

        return new RaptorOutputTableHandle(
                connectorId,
                transactionId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles.build(),
                columnTypes.build(),
                Optional.ofNullable(sampleWeightColumnHandle),
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST),
                temporalColumnHandle);
    }

    private static Optional<RaptorColumnHandle> getTemporalColumnHandle(String temporalColumn, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        if (temporalColumn == null) {
            return Optional.empty();
        }

        RaptorColumnHandle handle = columnHandleMap.get(temporalColumn);
        if (handle == null) {
            throw new PrestoException(NOT_FOUND, "Temporal column does not exist: " + temporalColumn);
        }
        return Optional.of(handle);
    }

    private static List<RaptorColumnHandle> getSortColumnHandles(List<String> sortColumns, Map<String, RaptorColumnHandle> columnHandleMap)
    {
        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        for (String column : sortColumns) {
            if (!columnHandleMap.containsKey(column)) {
                throw new PrestoException(NOT_FOUND, "Ordering column does not exist: " + column);
            }
            columnHandles.add(columnHandleMap.get(column));
        }
        return columnHandles.build();
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, Collection<Slice> fragments)
    {
        RaptorOutputTableHandle table = checkType(outputTableHandle, RaptorOutputTableHandle.class, "outputTableHandle");
        long transactionId = table.getTransactionId();

        long newTableId = runTransaction(dbi, (dbiHandle, status) -> {
            MetadataDao dao = dbiHandle.attach(MetadataDao.class);
            long tableId = dao.insertTable(table.getSchemaName(), table.getTableName(), true);
            List<RaptorColumnHandle> sortColumnHandles = table.getSortColumnHandles();

            for (int i = 0; i < table.getColumnTypes().size(); i++) {
                RaptorColumnHandle column = table.getColumnHandles().get(i);

                int columnId = i + 1;
                String type = table.getColumnTypes().get(i).getTypeSignature().toString();
                Integer sortPosition = sortColumnHandles.contains(column) ? sortColumnHandles.indexOf(column) : null;
                dao.insertColumn(tableId, columnId, column.getColumnName(), i, type, sortPosition);

                if (table.getTemporalColumnHandle().isPresent() && table.getTemporalColumnHandle().get().equals(column)) {
                    dao.updateTemporalColumnId(tableId, columnId);
                }
            }

            return tableId;
        });

        List<ColumnInfo> columns = table.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        // TODO: refactor this to avoid creating an empty table on failure
        shardManager.createTable(newTableId, columns);
        shardManager.commitShards(transactionId, newTableId, columns, parseFragments(fragments), Optional.empty());
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "outputTableHandle");
        shardManager.rollbackTransaction(handle.getTransactionId());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();

        ImmutableList.Builder<RaptorColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (TableColumn column : dao.getTableColumns(tableId)) {
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getColumnName(), column.getColumnId(), column.getDataType()));
            columnTypes.add(column.getDataType());
        }

        long transactionId = shardManager.beginTransaction();

        Optional<String> externalBatchId = getExternalBatchId(session);
        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(tableId);
        return new RaptorInsertTableHandle(connectorId,
                transactionId,
                tableId,
                columnHandles.build(),
                columnTypes.build(),
                externalBatchId,
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST));
    }

    private List<RaptorColumnHandle> getSortColumnHandles(long tableId)
    {
        return dao.listSortColumns(tableId).stream()
                .map(this::getRaptorColumnHandle)
                .collect(toList());
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        RaptorInsertTableHandle handle = checkType(insertHandle, RaptorInsertTableHandle.class, "insertHandle");
        long transactionId = handle.getTransactionId();
        long tableId = handle.getTableId();
        Optional<String> externalBatchId = handle.getExternalBatchId();
        List<ColumnInfo> columns = handle.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        shardManager.commitShards(transactionId, tableId, columns, parseFragments(fragments), externalBatchId);
    }

    @Override
    public void rollbackInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle)
    {
        RaptorInsertTableHandle handle = checkType(insertHandle, RaptorInsertTableHandle.class, "insertHandle");
        shardManager.rollbackTransaction(handle.getTransactionId());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return shardRowIdHandle(connectorId);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");

        long transactionId = shardManager.beginTransaction();

        return new RaptorTableHandle(
                connectorId,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableId(),
                OptionalLong.of(transactionId),
                handle.getSampleWeightColumnHandle());
    }

    @Override
    public void commitDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        long transactionId = table.getTransactionId().getAsLong();
        long tableId = table.getTableId();

        List<ColumnInfo> columns = getColumnHandles(session, tableHandle).values().stream()
                .map(handle -> checkType(handle, RaptorColumnHandle.class, "columnHandle"))
                .map(ColumnInfo::fromHandle).collect(toList());

        ImmutableSet.Builder<UUID> oldShardUuids = ImmutableSet.builder();
        ImmutableList.Builder<ShardInfo> newShards = ImmutableList.builder();

        fragments.stream()
                .map(fragment -> shardDeltaCodec.fromJson(fragment.getBytes()))
                .forEach(delta -> {
                    oldShardUuids.addAll(delta.getOldShardUuids());
                    newShards.addAll(delta.getNewShards());
                });

        shardManager.replaceShardUuids(transactionId, tableId, columns, oldShardUuids.build(), newShards.build());
    }

    @Override
    public void rollbackDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        shardManager.rollbackTransaction(handle.getTransactionId().getAsLong());
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return false;
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();

        if (replace) {
            runTransaction(dbi, (handle, status) -> {
                MetadataDao dao = handle.attach(MetadataDao.class);
                dao.dropView(schemaName, tableName);
                dao.insertView(schemaName, tableName, viewData);
                return null;
            });
            return;
        }

        try {
            dao.insertView(schemaName, tableName, viewData);
        }
        catch (PrestoException e) {
            if (viewExists(session, viewName)) {
                throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }
            throw e;
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (!viewExists(session, viewName)) {
            throw new ViewNotFoundException(viewName);
        }
        dao.dropView(viewName.getSchemaName(), viewName.getTableName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return dao.listViews(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> map = ImmutableMap.builder();
        for (ViewResult view : dao.getViews(prefix.getSchemaName(), prefix.getTableName())) {
            map.put(view.getName(), new ConnectorViewDefinition(view.getName(), Optional.empty(), view.getData()));
        }
        return map.build();
    }

    private boolean viewExists(ConnectorSession session, SchemaTableName viewName)
    {
        return !getViews(session, viewName.toSchemaTablePrefix()).isEmpty();
    }

    private RaptorColumnHandle getRaptorColumnHandle(TableColumn tableColumn)
    {
        return new RaptorColumnHandle(connectorId, tableColumn.getColumnName(), tableColumn.getColumnId(), tableColumn.getDataType());
    }

    private Collection<ShardInfo> parseFragments(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(fragment -> shardInfoCodec.fromJson(fragment.getBytes()))
                .collect(toList());
    }

    private static Predicate<ColumnMetadata> isSampleWeightColumn()
    {
        return input -> input.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME);
    }

    private static ColumnMetadata hiddenColumn(String name, Type type)
    {
        return new ColumnMetadata(name, type, false, null, true);
    }
}
