package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class DatabaseMetadata
        implements Metadata
{
    private final IDBI dbi;
    private final MetadataDao dao;
    private final FunctionRegistry functions = new FunctionRegistry();

    @Inject
    public DatabaseMetadata(@ForMetadata IDBI dbi)
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(MetadataDao.class);
        createTables();
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return functions.get(name, parameterTypes);
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkArgument(catalogName.equals(catalogName.toLowerCase()), "catalogName is not lowercase");
        checkArgument(schemaName.equals(schemaName.toLowerCase()), "schemaName is not lowercase");
        checkArgument(tableName.equals(tableName.toLowerCase()), "tableName is not lowercase");

        Long tableId = dao.getTableId(catalogName, schemaName, tableName);
        if (tableId == null) {
            return null;
        }
        TableHandle tableHandle = new NativeTableHandle(tableId);

        List<ColumnMetadata> columns = dao.getTableColumnMetaData(tableId);
        if (columns.isEmpty()) {
            return null;
        }

        return new TableMetadata(catalogName, schemaName, tableName, columns, tableHandle);
    }

    @Override
    public TableMetadata getTable(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        NativeTableHandle nativeTableHandle = (NativeTableHandle) tableHandle;

        TableNamespace tableNamespace = dao.getTableNamespace(nativeTableHandle.getTableId());

        List<ColumnMetadata> columns = dao.getTableColumnMetaData(nativeTableHandle.getTableId());
        if (columns.isEmpty()) {
            return null;
        }

        return new TableMetadata(
                tableNamespace.getCatalogName(),
                tableNamespace.getDatabaseName(),
                tableNamespace.getTableName(),
                columns,
                tableHandle);
    }

    @Override
    public ColumnMetadata getColumn(ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");

        NativeColumnHandle nativeColumnHandle = (NativeColumnHandle) columnHandle;
        return dao.getColumnMetaData(nativeColumnHandle.getColumnId());
    }

    @Override
    public void createTable(final TableMetadata table)
    {
        checkArgument(!dao.tableExists(table), "Table '%s.%s.%s' already defined",
                table.getSchemaName(), table.getCatalogName(), table.getTableName());

        // TODO: handle already exists SQLException (SQLState = 23505)
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                MetadataDao dao = handle.attach(MetadataDao.class);
                long tableId = dao.insertTable(table);
                int position = 1;
                for (ColumnMetadata column : table.getColumns()) {
                    dao.insertColumn(tableId, column.getName(), position, column.getType().getName());
                    position++;
                }
            }
        });
    }

    private void createTables()
    {
        dao.createTablesTable();
        dao.createColumnsTable();
    }
}
