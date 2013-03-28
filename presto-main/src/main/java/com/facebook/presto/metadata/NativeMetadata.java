package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NativeMetadata
        implements Metadata
{
    private static final Logger log = Logger.get(NativeMetadata.class);

    private final IDBI dbi;
    private final MetadataDao dao;
    private final FunctionRegistry functions = new FunctionRegistry();

    @Inject
    public NativeMetadata(@ForMetadata IDBI dbi)
            throws InterruptedException
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(MetadataDao.class);

        // keep retrying if database is unavailable when the server starts
        createTablesWithRetry();
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return functions.get(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return functions.get(handle);
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return functions.list();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        return dao.listSchemaNames(catalogName);
    }

    @Override
    public TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);

        Long tableId = dao.getTableId(table);
        if (tableId == null) {
            return null;
        }
        TableHandle tableHandle = new NativeTableHandle(tableId);

        List<ColumnMetadata> columns = dao.getTableColumnMetaData(tableId);
        if (columns.isEmpty()) {
            return null;
        }

        return new TableMetadata(table, columns, tableHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        return dao.listTables(prefix.getCatalogName(), prefix.getSchemaName().orNull());
    }

    @Override
    public List<TableColumn> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        return dao.listTableColumns(prefix.getCatalogName(),
                prefix.getSchemaName().orNull(),
                prefix.getTableName().orNull());
    }

    @Override
    public List<String> listTablePartitionKeys(QualifiedTableName table)
    {
        checkTable(table);
        return ImmutableList.of();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        return ImmutableList.of();
    }

    @Override
    public QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(DataSourceType.NATIVE == tableHandle.getDataSourceType(), "not a native handle: %s", tableHandle);

        long tableId = ((NativeTableHandle) tableHandle).getTableId();

        QualifiedTableName tableName = dao.getTableName(tableId);
        checkState(tableName != null, "no table with id %s exists", tableId);
        return tableName;
    }

    @Override
    public TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkState(DataSourceType.NATIVE == tableHandle.getDataSourceType(), "not a native handle: %s", tableHandle);
        checkState(DataSourceType.NATIVE == columnHandle.getDataSourceType(), "not a native handle: %s", columnHandle);

        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();

        TableColumn tableColumn = dao.getTableColumn(columnId);
        checkState(tableColumn != null, "no column with id %s exists", columnId);
        return tableColumn;
    }

    @Override
    public void createTable(final TableMetadata table)
    {
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(final Handle handle, TransactionStatus status)
                    throws Exception
            {
                // Ignore exception if table already exists
                runIgnoringConstraintViolation(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        MetadataDao dao = handle.attach(MetadataDao.class);
                        long tableId = dao.insertTable(table.getTable());
                        int position = 1;
                        for (ColumnMetadata column : table.getColumns()) {
                            dao.insertColumn(tableId, column.getName(), position, column.getType().getName());
                            position++;
                        }
                    }
                });
            }
        });
    }

    private void createTablesWithRetry()
            throws InterruptedException
    {
        Duration delay = new Duration(10, TimeUnit.SECONDS);
        while (true) {
            try {
                createTables();
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                Thread.sleep((long) delay.toMillis());
            }
        }
    }

    private void createTables()
    {
        dao.createTablesTable();
        dao.createColumnsTable();
    }
}
