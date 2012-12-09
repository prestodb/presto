package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public abstract class AbstractMetadata
        implements Metadata
{
    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public abstract TableMetadata getTable(String catalogName, String schemaName, String tableName);

    @Override
    public List<QualifiedTableName> listTables(String catalogName, String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(TableMetadata table)
    {
        throw new UnsupportedOperationException();
    }
}
