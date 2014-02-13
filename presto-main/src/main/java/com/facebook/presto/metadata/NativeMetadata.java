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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.facebook.presto.metadata.MetadataDaoUtils.createMetadataTablesWithRetry;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NativeMetadata
        implements ConnectorMetadata
{
    private final IDBI dbi;
    private final MetadataDao dao;
    private final ShardManager shardManager;
    private final String connectorId;

    @Inject
    public NativeMetadata(NativeConnectorId connectorId, @ForMetadata IDBI dbi, ShardManager shardManager)
    {
        checkNotNull(connectorId, "connectorId is null");

        this.connectorId = connectorId.toString();
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(MetadataDao.class);
        this.shardManager = checkNotNull(shardManager, "shardManager is null");

        createMetadataTablesWithRetry(dao);
    }

    @Override
    public List<String> listSchemaNames()
    {
        return dao.listSchemaNames(connectorId);
    }

    @Override
    public ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(connectorId, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        Long columnId = dao.getColumnId(table.getTableId(), NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME);
        NativeColumnHandle sampleWeightColumnHandle = null;
        if (columnId != null) {
            sampleWeightColumnHandle = new NativeColumnHandle(NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME, columnId);
        }
        return new NativeTableHandle(tableName.getSchemaName(), tableName.getTableName(), table.getTableId(), sampleWeightColumnHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        NativeTableHandle nativeTableHandle = (NativeTableHandle) tableHandle;

        SchemaTableName tableName = getTableName(tableHandle);
        checkArgument(tableName != null, "Table %s does not exist", tableName);
        List<ColumnMetadata> columns = dao.getTableColumnMetaData(nativeTableHandle.getTableId());
        columns = ImmutableList.copyOf(Iterables.filter(columns, new Predicate<ColumnMetadata>() {
            @Override
            public boolean apply(ColumnMetadata input)
            {
                return !input.getName().equals(NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME);
            }
        }));
        checkArgument(!columns.isEmpty(), "Table %s does not have any columns", tableName);
        if (columns.isEmpty()) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(@Nullable String schemaNameOrNull)
    {
        return dao.listTables(connectorId, schemaNameOrNull);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        NativeTableHandle nativeTableHandle = (NativeTableHandle) tableHandle;

        ImmutableMap.Builder<String, ConnectorColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(nativeTableHandle.getTableId())) {
            if (tableColumn.getColumnName().equals(NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            builder.put(tableColumn.getColumnName(), new NativeColumnHandle(tableColumn.getColumnName(), tableColumn.getColumnId()));
        }
        return builder.build();
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        NativeTableHandle nativeTableHandle = (NativeTableHandle) tableHandle;

        Long columnId = dao.getColumnId(nativeTableHandle.getTableId(), columnName);
        if (columnId == null) {
            return null;
        }
        return new NativeColumnHandle(columnName, columnId);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        return ((NativeTableHandle) tableHandle).getSampleWeightColumnHandle();
    }

    @Override
    public boolean canCreateSampledTables()
    {
        return true;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        checkArgument(columnHandle instanceof NativeColumnHandle, "columnHandle is not an instance of NativeColumnHandle");

        long tableId = ((NativeTableHandle) tableHandle).getTableId();
        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();

        ColumnMetadata columnMetadata = dao.getColumnMetadata(tableId, columnId);
        checkState(columnMetadata != null, "no column with id %s exists", columnId);
        return columnMetadata;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(connectorId, prefix.getSchemaName(), prefix.getTableName())) {
            if (tableColumn.getColumnName().equals(NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME)) {
                continue;
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType(), tableColumn.getOrdinalPosition(), false);
            columns.put(tableColumn.getTable().asSchemaTableName(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    private SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");

        long tableId = ((NativeTableHandle) tableHandle).getTableId();

        SchemaTableName tableName = dao.getTableName(tableId).asSchemaTableName();
        checkState(tableName != null, "no table with id %s exists", tableId);
        return tableName;
    }

    @Override
    public ConnectorTableHandle createTable(final ConnectorTableMetadata tableMetadata)
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
                            dao.insertColumn(tableId, columnId, column.getName(), ordinalPosition, column.getType().getName());
                            ordinalPosition++;
                        }
                        if (tableMetadata.isSampled()) {
                            dao.insertColumn(tableId, ordinalPosition + 1, NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME, ordinalPosition, BIGINT.getName());
                        }
                        return tableId;
                    }
                }, null);
            }
        });
        checkState(tableId != null, "table %s already exists", tableMetadata.getTable());
        NativeColumnHandle sampleWeightColumnHandle = null;
        if (tableMetadata.isSampled()) {
            sampleWeightColumnHandle = new NativeColumnHandle(NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME, tableMetadata.getColumns().size() + 1);
        }
        return new NativeTableHandle(tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName(), tableId, sampleWeightColumnHandle);
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        final long tableId = ((NativeTableHandle) tableHandle).getTableId();
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                MetadataDaoUtils.dropTable(dao, tableId);
            }
        });
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<NativeColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        long maxColumnId = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            long columnId = column.getOrdinalPosition() + 1;
            maxColumnId = Math.max(maxColumnId, columnId);
            columnHandles.add(new NativeColumnHandle(column.getName(), columnId));
            columnTypes.add(column.getType());
        }
        NativeColumnHandle sampleWeightColumnHandle = null;
        if (tableMetadata.isSampled()) {
            sampleWeightColumnHandle = new NativeColumnHandle(NativeColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME, maxColumnId + 1);
            columnHandles.add(sampleWeightColumnHandle);
            columnTypes.add(BIGINT);
        }

        return new NativeOutputTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnHandles.build(),
                columnTypes.build(),
                sampleWeightColumnHandle);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle outputTableHandle, Collection<String> fragments)
    {
        final NativeOutputTableHandle table = checkType(outputTableHandle, NativeOutputTableHandle.class, "outputTableHandle");

        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle dbiHandle, TransactionStatus status)
            {
                MetadataDao dao = dbiHandle.attach(MetadataDao.class);
                long tableId = dao.insertTable(connectorId, table.getSchemaName(), table.getTableName());
                for (int i = 0; i < table.getColumnTypes().size(); i++) {
                    NativeColumnHandle column = table.getColumnHandles().get(i);
                    Type columnType = table.getColumnTypes().get(i);
                    dao.insertColumn(tableId, i + 1, column.getColumnName(), i, columnType.getName());
                }
            }
        });

        ImmutableMap.Builder<UUID, String> shards = ImmutableMap.builder();
        for (String fragment : fragments) {
            Iterator<String> split = Splitter.on(':').split(fragment).iterator();
            String nodeId = split.next();
            UUID shardUuid = UUID.fromString(split.next());
            shards.put(shardUuid, nodeId);
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(table.getSchemaName(), table.getTableName()));

        shardManager.commitUnpartitionedTable(tableHandle, shards.build());
    }
}
