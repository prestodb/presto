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
import com.facebook.presto.raptor.metadata.MetadataDaoUtils;
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
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.raptor.RaptorColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.metadata.MetadataDaoUtils.createMetadataTablesWithRetry;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final String connectorId;

    @Inject
    public RaptorMetadata(RaptorConnectorId connectorId, @ForMetadata IDBI dbi, ShardManager shardManager, JsonCodec<ShardInfo> shardInfoCodec)
    {
        checkNotNull(connectorId, "connectorId is null");

        this.connectorId = connectorId.toString();
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(MetadataDao.class);
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.shardInfoCodec = checkNotNull(shardInfoCodec, "shardInfoCodec is null");

        createMetadataTablesWithRetry(dao);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return dao.listSchemaNames(connectorId);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(connectorId, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        List<TableColumn> tableColumns = dao.getTableColumns(table.getTableId());
        checkArgument(!tableColumns.isEmpty(), "Table %s does not have any columns", tableName);

        RaptorColumnHandle countColumnHandle = null;
        RaptorColumnHandle sampleWeightColumnHandle = null;
        for (TableColumn tableColumn : tableColumns) {
            if (SAMPLE_WEIGHT_COLUMN_NAME.equals(tableColumn.getColumnName())) {
                sampleWeightColumnHandle = getRaptorColumnHandle(tableColumn);
            }
            if (countColumnHandle == null && tableColumn.getDataType().getJavaType().isPrimitive()) {
                countColumnHandle = getRaptorColumnHandle(tableColumn);
            }
        }

        if (sampleWeightColumnHandle != null) {
            sampleWeightColumnHandle = new RaptorColumnHandle(connectorId, SAMPLE_WEIGHT_COLUMN_NAME, sampleWeightColumnHandle.getColumnId(), BIGINT);
        }
        return new RaptorTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTableId(),
                sampleWeightColumnHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle handle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        List<ColumnMetadata> columns = dao.getTableColumns(handle.getTableId()).stream()
                .map(TableColumn::toColumnMetadata)
                .filter(isSampleWeightColumn().negate())
                .collect(toList());
        if (columns.isEmpty()) {
            throw new PrestoException(RAPTOR_ERROR, "Table does not have any columns: " + tableName);
        }
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return dao.listTables(connectorId, schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(raptorTableHandle.getTableId())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            builder.put(tableColumn.getColumnName(), getRaptorColumnHandle(tableColumn));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getSampleWeightColumnHandle();
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();
        long columnId = checkType(columnHandle, RaptorColumnHandle.class, "columnHandle").getColumnId();

        TableColumn tableColumn = dao.getTableColumn(tableId, columnId);
        if (tableColumn == null) {
            throw new PrestoException(NOT_FOUND, format("Column ID %s does not exist for table ID %s", columnId, tableId));
        }
        return tableColumn.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(connectorId, prefix.getSchemaName(), prefix.getTableName())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType(), tableColumn.getOrdinalPosition(), false);
            columns.put(tableColumn.getTable(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        commitCreateTable(beginCreateTable(session, tableMetadata), ImmutableList.of());
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        dbi.inTransaction((handle, status) -> {
            shardManager.dropTableShards(raptorHandle.getTableId());
            MetadataDaoUtils.dropTable(dao, raptorHandle.getTableId());
            return null;
        });
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        dbi.inTransaction((handle, status) -> {
            MetadataDao dao = handle.attach(MetadataDao.class);
            dao.renameTable(table.getTableId(), newTableName.getSchemaName(), newTableName.getTableName());
            return null;
        });
    }

    @Override
    public void renameColumn(ConnectorTableHandle tableHandle, ColumnHandle source, String target)
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
        long maxColumnId = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            long columnId = column.getOrdinalPosition() + 1;
            maxColumnId = Math.max(maxColumnId, columnId);
            columnHandles.add(new RaptorColumnHandle(connectorId, column.getName(), columnId, column.getType()));
            columnTypes.add(column.getType());
        }
        RaptorColumnHandle sampleWeightColumnHandle = null;
        if (tableMetadata.isSampled()) {
            sampleWeightColumnHandle = new RaptorColumnHandle(connectorId, SAMPLE_WEIGHT_COLUMN_NAME, maxColumnId + 1, BIGINT);
            columnHandles.add(sampleWeightColumnHandle);
            columnTypes.add(BIGINT);
        }

        return new RaptorOutputTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles.build(),
                columnTypes.build(),
                sampleWeightColumnHandle,
                ImmutableList.of(),
                ImmutableList.of());
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle outputTableHandle, Collection<Slice> fragments)
    {
        RaptorOutputTableHandle table = checkType(outputTableHandle, RaptorOutputTableHandle.class, "outputTableHandle");

        long newTableId = dbi.inTransaction((dbiHandle, status) -> {
            MetadataDao dao = dbiHandle.attach(MetadataDao.class);
            long tableId = dao.insertTable(connectorId, table.getSchemaName(), table.getTableName());
            for (int i = 0; i < table.getColumnTypes().size(); i++) {
                RaptorColumnHandle column = table.getColumnHandles().get(i);
                Type columnType = table.getColumnTypes().get(i);
                dao.insertColumn(tableId, i + 1, column.getColumnName(), i, columnType.getTypeSignature().toString());
            }
            return tableId;
        });

        List<ColumnInfo> columns = table.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        // TODO: refactor this to avoid creating an empty table on failure
        shardManager.createTable(newTableId, columns);
        shardManager.commitShards(newTableId, columns, parseFragments(fragments), Optional.empty());
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

        String externalBatchId = session.getProperties().get("external_batch_id");
        List<RaptorColumnHandle> sortColumnHandles = getSortColumnHandles(tableId);
        return new RaptorInsertTableHandle(connectorId,
                tableId,
                columnHandles.build(),
                columnTypes.build(),
                externalBatchId,
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST));
    }

    private List<RaptorColumnHandle> getSortColumnHandles(long tableId)
    {
        ImmutableList.Builder<RaptorColumnHandle> builder = ImmutableList.builder();
        for (TableColumn tableColumn : dao.listSortColumns(tableId)) {
            checkArgument(!tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME), "sample weight column may not be a sort column");
            builder.add(getRaptorColumnHandle(tableColumn));
        }
        return builder.build();
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        RaptorInsertTableHandle handle = checkType(insertHandle, RaptorInsertTableHandle.class, "insertHandle");
        long tableId = handle.getTableId();
        Optional<String> externalBatchId = Optional.ofNullable(handle.getExternalBatchId());
        List<ColumnInfo> columns = handle.getColumnHandles().stream().map(ColumnInfo::fromHandle).collect(toList());

        shardManager.commitShards(tableId, columns, parseFragments(fragments), externalBatchId);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();

        if (replace) {
            dbi.inTransaction((handle, status) -> {
                MetadataDao dao = handle.attach(MetadataDao.class);
                dao.dropView(connectorId, schemaName, tableName);
                dao.insertView(connectorId, schemaName, tableName, viewData);
                return null;
            });
            return;
        }

        try {
            dao.insertView(connectorId, schemaName, tableName, viewData);
        }
        catch (UnableToExecuteStatementException e) {
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
        dao.dropView(connectorId, viewName.getSchemaName(), viewName.getTableName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return dao.listViews(connectorId, schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, String> map = ImmutableMap.builder();
        for (ViewResult view : dao.getViews(connectorId, prefix.getSchemaName(), prefix.getTableName())) {
            map.put(view.getName(), view.getData());
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
}
