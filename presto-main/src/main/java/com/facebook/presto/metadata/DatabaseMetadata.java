package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.LongAverageAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.google.common.collect.ImmutableMap;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class DatabaseMetadata
        implements Metadata
{
    private static final Map<String, FunctionInfo> FUNCTIONS = ImmutableMap.<String, FunctionInfo>builder()
            .put("COUNT", new FunctionInfo(true, CountAggregation.PROVIDER))
            .put("SUM", new FunctionInfo(true, LongSumAggregation.PROVIDER))
            .put("AVG", new FunctionInfo(true, LongAverageAggregation.PROVIDER))
            .build();

    private final IDBI dbi;
    private final MetadataDao dao;

    @Inject
    public DatabaseMetadata(@ForMetadata IDBI dbi)
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(MetadataDao.class);
        createTables();
    }

    @Override
    public FunctionInfo getFunction(String name)
    {
        checkArgument(name.equals(name.toLowerCase()), "name is not lowercase");
        return FUNCTIONS.get(name);
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkArgument(catalogName.equals(catalogName.toLowerCase()), "catalogName is not lowercase");
        checkArgument(schemaName.equals(schemaName.toLowerCase()), "schemaName is not lowercase");
        checkArgument(tableName.equals(tableName.toLowerCase()), "tableName is not lowercase");

        List<ColumnMetadata> columns = dao.getColumnMetaData(catalogName, schemaName, tableName);
        if (columns.isEmpty()) {
            return null;
        }
        return new TableMetadata(catalogName, schemaName, tableName, columns);
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
