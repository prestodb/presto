package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class DatabaseMetadata
        implements Metadata
{
    private final IDBI dbi;
    private final MetadataDao dao;
    private final Multimap<QualifiedName, FunctionInfo> functions;

    @Inject
    public DatabaseMetadata(@ForMetadata IDBI dbi)
    {
        this.dbi = dbi;
        this.dao = dbi.onDemand(MetadataDao.class);
        createTables();

        functions = buildFunctions(
                new FunctionInfo(QualifiedName.of("count"), true, TupleInfo.Type.FIXED_INT_64, ImmutableList.<TupleInfo.Type>of(), null, null),
                new FunctionInfo(QualifiedName.of("sum"), true, TupleInfo.Type.FIXED_INT_64, ImmutableList.of(TupleInfo.Type.FIXED_INT_64), null, null),
                new FunctionInfo(QualifiedName.of("sum"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.DOUBLE), null, null),
                new FunctionInfo(QualifiedName.of("avg"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.DOUBLE), null, null),
                new FunctionInfo(QualifiedName.of("avg"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.FIXED_INT_64), null, null)
        );
    }

    private Multimap<QualifiedName, FunctionInfo> buildFunctions(FunctionInfo... infos)
    {
        return Multimaps.index(ImmutableList.copyOf(infos), new Function<FunctionInfo, QualifiedName>()
        {
            @Override
            public QualifiedName apply(FunctionInfo input)
            {
                return input.getName();
            }
        });
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        for (FunctionInfo functionInfo : functions.get(name)) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        throw new IllegalArgumentException(format("Function %s(%s) not registered", name, Joiner.on(", ").join(parameterTypes)));
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
