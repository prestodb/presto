package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface Metadata
{
    FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes);

    TableMetadata getTable(String catalogName, String schemaName, String tableName);

    TableMetadata getTable(TableHandle tableHandle);

    ColumnMetadata getColumn(ColumnHandle columnHandle);

    void createTable(TableMetadata table);
}
