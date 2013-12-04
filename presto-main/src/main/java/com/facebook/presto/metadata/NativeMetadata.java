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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.facebook.presto.metadata.MetadataDaoUtils.createMetadataTablesWithRetry;
import static com.facebook.presto.tuple.TupleInfo.Type.fromColumnType;
import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NativeMetadata
        implements ConnectorMetadata
{
    private final IDBI dbi;
    private final MetadataDao dao;
    private final String catalogName;

    public NativeMetadata(String catalogName, IDBI dbi)
    {
        this.catalogName = catalogName;
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(MetadataDao.class);

        createMetadataTablesWithRetry(dao);
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof NativeTableHandle;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return dao.listSchemaNames(catalogName);
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(catalogName, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new NativeTableHandle(tableName.getSchemaName(), tableName.getTableName(), table.getTableId());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        NativeTableHandle nativeTableHandle = (NativeTableHandle) tableHandle;

        SchemaTableName tableName = getTableName(tableHandle);
        checkArgument(tableName != null, "Table %s does not exist", tableName);
        List<ColumnMetadata> columns = dao.getTableColumnMetaData(nativeTableHandle.getTableId());
        checkArgument(!columns.isEmpty(), "Table %s does not have any columns", tableName);
        if (columns.isEmpty()) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(@Nullable String schemaNameOrNull)
    {
        return dao.listTables(catalogName, schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        NativeTableHandle nativeTableHandle = (NativeTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(nativeTableHandle.getTableId())) {
            builder.put(tableColumn.getColumnName(), new NativeColumnHandle(tableColumn.getColumnName(), tableColumn.getColumnId()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
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
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");
        checkArgument(columnHandle instanceof NativeColumnHandle, "columnHandle is not an instance of NativeColumnHandle");

        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();

        ColumnMetadata columnMetadata = dao.getColumnMetadata(columnId);
        checkState(columnMetadata != null, "no column with id %s exists", columnId);
        return columnMetadata;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(catalogName, prefix.getSchemaName(), prefix.getTableName())) {
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType().toColumnType(), tableColumn.getOrdinalPosition(), false);
            columns.put(tableColumn.getTable().asSchemaTableName(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    private SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof NativeTableHandle, "tableHandle is not an instance of NativeTableHandle");

        long tableId = ((NativeTableHandle) tableHandle).getTableId();

        SchemaTableName tableName = dao.getTableName(tableId).asSchemaTableName();
        checkState(tableName != null, "no table with id %s exists", tableId);
        return tableName;
    }

    @Override
    public TableHandle createTable(final ConnectorTableMetadata tableMetadata)
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
                        long tableId = dao.insertTable(catalogName, tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName());
                        int ordinalPosition = 0;
                        for (ColumnMetadata column : tableMetadata.getColumns()) {
                            dao.insertColumn(tableId, column.getName(), ordinalPosition, fromColumnType(column.getType()).getName());
                            ordinalPosition++;
                        }
                        return tableId;
                    }
                }, null);
            }
        });
        checkState(tableId != null, "table %s already exists", tableMetadata.getTable());
        return new NativeTableHandle(tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName(), tableId);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
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
}
