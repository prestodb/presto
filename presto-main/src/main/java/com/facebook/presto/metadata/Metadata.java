package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

public interface Metadata
{
    FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes);

    FunctionInfo getFunction(FunctionHandle handle);

    TableMetadata getTable(String catalogName, String schemaName, String tableName);

    List<QualifiedTableName> listTables(String catalogName);

    List<QualifiedTableName> listTables(String catalogName, String schemaName);

    List<TableColumn> listTableColumns(String catalogName);

    void createTable(TableMetadata table);
}
