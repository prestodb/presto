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

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.util.SqlUtils.runIgnoringConstraintViolation;

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
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);

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
    public List<QualifiedTableName> listTables(String catalogName)
    {
        checkCatalogName(catalogName);
        return dao.listTables(catalogName, null);
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, String schemaName)
    {
        checkSchemaName(catalogName, schemaName);
        return dao.listTables(catalogName, schemaName);
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName)
    {
        checkCatalogName(catalogName);
        return dao.listTableColumns(catalogName, null, null);
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName)
    {
        checkSchemaName(catalogName, schemaName);
        return dao.listTableColumns(catalogName, schemaName, null);
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        return dao.listTableColumns(catalogName, schemaName, tableName);
    }

    @Override
    public List<String> listTablePartitionKeys(String catalogName, String schemaName, String tableName)
    {
        return ImmutableList.of();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(String catalogName, String schemaName, String tableName)
    {
        return ImmutableList.of();
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
                        long tableId = dao.insertTable(table);
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
