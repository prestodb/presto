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

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.MetadataDaoUtils;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardNode;
import com.facebook.presto.raptor.metadata.Table;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.ViewResult;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.facebook.presto.raptor.RaptorColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.raptor.metadata.MetadataDaoUtils.createMetadataTablesWithRetry;
import static com.facebook.presto.raptor.metadata.SqlUtils.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;

public class RaptorMetadata
        implements ConnectorMetadata
{
    private static final Splitter NODE_SHARD_SPLITTER = Splitter.on(':').limit(2);
    private static final Splitter SHARD_SPLITTER = Splitter.on(',');

    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final String connectorId;

    @Inject
    public RaptorMetadata(RaptorConnectorId connectorId, @ForMetadata IDBI dbi, ShardManager shardManager)
    {
        checkNotNull(connectorId, "connectorId is null");

        this.connectorId = connectorId.toString();
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(MetadataDao.class);
        this.shardManager = checkNotNull(shardManager, "shardManager is null");

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
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        SchemaTableName tableName = new SchemaTableName(raptorTableHandle.getSchemaName(), raptorTableHandle.getTableName());
        List<ColumnMetadata> columns = FluentIterable
                .from(dao.getTableColumns(raptorTableHandle.getTableId()))
                .transform(TableColumn.columnMetadataGetter())
                .filter(not(isSampleWeightColumn()))
                .toList();
        checkArgument(!columns.isEmpty(), "Table %s does not have any columns", tableName);
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, @Nullable String schemaNameOrNull)
    {
        return dao.listTables(connectorId, schemaNameOrNull);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        RaptorTableHandle raptorTableHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        ImmutableMap.Builder<String, ConnectorColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(raptorTableHandle.getTableId())) {
            if (tableColumn.getColumnName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            builder.put(tableColumn.getColumnName(), getRaptorColumnHandle(tableColumn));
        }
        return builder.build();
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getSampleWeightColumnHandle();
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        long tableId = checkType(tableHandle, RaptorTableHandle.class, "tableHandle").getTableId();
        long columnId = checkType(columnHandle, RaptorColumnHandle.class, "columnHandle").getColumnId();

        TableColumn tableColumn = dao.getTableColumn(tableId, columnId);
        checkState(tableColumn != null, "no column with id %s exists", columnId);
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
    public ConnectorTableHandle createTable(ConnectorSession session, final ConnectorTableMetadata tableMetadata)
    {
        Long tableId = dbi.inTransaction(new TransactionCallback<Long>()
        {
            @Override
            public Long inTransaction(final Handle handle, TransactionStatus status)
                    throws Exception
            {
                // Ignore exception if table already exists
                return runIgnoringConstraintViolation(new Callable<Long>()
                {
                    @Override
                    public Long call()
                            throws Exception
                    {
                        MetadataDao dao = handle.attach(MetadataDao.class);
                        long tableId = dao.insertTable(connectorId, tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName());
                        int ordinalPosition = 0;
                        for (ColumnMetadata column : tableMetadata.getColumns()) {
                            long columnId = ordinalPosition + 1;
                            dao.insertColumn(tableId, columnId, column.getName(), ordinalPosition, column.getType().getTypeSignature().toString());
                            ordinalPosition++;
                        }
                        if (tableMetadata.isSampled()) {
                            dao.insertColumn(tableId, ordinalPosition + 1, SAMPLE_WEIGHT_COLUMN_NAME, ordinalPosition, StandardTypes.BIGINT);
                        }
                        return tableId;
                    }
                }, null);
            }
        });
        checkState(tableId != null, "table %s already exists", tableMetadata.getTable());
        RaptorColumnHandle sampleWeightColumnHandle = null;
        if (tableMetadata.isSampled()) {
            sampleWeightColumnHandle = new RaptorColumnHandle(connectorId, SAMPLE_WEIGHT_COLUMN_NAME, tableMetadata.getColumns().size() + 1, BIGINT);
        }

        return new RaptorTableHandle(
                connectorId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableId,
                sampleWeightColumnHandle);
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        final RaptorTableHandle raptorHandle = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                shardManager.dropTableShards(raptorHandle.getTableId());
                MetadataDaoUtils.dropTable(dao, raptorHandle.getTableId());
            }
        });
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, final SchemaTableName newTableName)
    {
        final RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                MetadataDao dao = handle.attach(MetadataDao.class);
                dao.renameTable(table.getTableId(), newTableName.getSchemaName(), newTableName.getTableName());
            }
        });
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
                sampleWeightColumnHandle);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle outputTableHandle, Collection<String> fragments)
    {
        final RaptorOutputTableHandle table = checkType(outputTableHandle, RaptorOutputTableHandle.class, "outputTableHandle");

        long tableId = dbi.inTransaction(new TransactionCallback<Long>()
        {
            @Override
            public Long inTransaction(Handle dbiHandle, TransactionStatus status)
            {
                MetadataDao dao = dbiHandle.attach(MetadataDao.class);
                long tableId = dao.insertTable(connectorId, table.getSchemaName(), table.getTableName());
                for (int i = 0; i < table.getColumnTypes().size(); i++) {
                    RaptorColumnHandle column = table.getColumnHandles().get(i);
                    Type columnType = table.getColumnTypes().get(i);
                    dao.insertColumn(tableId, i + 1, column.getColumnName(), i, columnType.getTypeSignature().toString());
                }
                return tableId;
            }
        });

        shardManager.commitTable(tableId, parseFragments(fragments), Optional.empty());
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

        return new RaptorInsertTableHandle(connectorId, tableId, columnHandles.build(), columnTypes.build(), externalBatchId);
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<String> fragments)
    {
        RaptorInsertTableHandle handle = checkType(insertHandle, RaptorInsertTableHandle.class, "insertHandle");
        long tableId = handle.getTableId();
        Optional<String> externalBatchId = Optional.ofNullable(handle.getExternalBatchId());

        shardManager.commitTable(tableId, parseFragments(fragments), externalBatchId);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, final String viewData, boolean replace)
    {
        final String schemaName = viewName.getSchemaName();
        final String tableName = viewName.getTableName();

        if (replace) {
            dbi.inTransaction(new VoidTransactionCallback()
            {
                @Override
                protected void execute(Handle handle, TransactionStatus status)
                        throws Exception
                {
                    MetadataDao dao = handle.attach(MetadataDao.class);
                    dao.dropView(connectorId, schemaName, tableName);
                    dao.insertView(connectorId, schemaName, tableName, viewData);
                }
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

    private static List<ShardNode> parseFragments(Collection<String> fragments)
    {
        // Format of each fragment: nodeId:shardUuid1,shardUuid2,shardUuid3
        ImmutableList.Builder<ShardNode> shards = ImmutableList.builder();
        for (String fragment : fragments) {
            Iterator<String> split = NODE_SHARD_SPLITTER.split(fragment).iterator();
            String nodeId = split.next();
            checkArgument(split.hasNext(), "fragment not formatted correctly");
            String uuids = split.next();
            for (String uuidString : SHARD_SPLITTER.trimResults().omitEmptyStrings().split(uuids)) {
                UUID shardUuid = UUID.fromString(uuidString);
                shards.add(new ShardNode(shardUuid, nodeId));
            }
        }
        return shards.build();
    }

    private static Predicate<ColumnMetadata> isSampleWeightColumn()
    {
        return new Predicate<ColumnMetadata>()
        {
            @Override
            public boolean apply(ColumnMetadata input)
            {
                return input.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME);
            }
        };
    }
}
