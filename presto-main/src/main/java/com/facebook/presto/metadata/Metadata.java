package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;
import java.util.Map;

public interface Metadata
{
    FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes);

    FunctionInfo getFunction(FunctionHandle handle);

    List<FunctionInfo> listFunctions();

    List<String> listSchemaNames(String catalogName);

    TableMetadata getTable(String catalogName, String schemaName, String tableName);

    List<QualifiedTableName> listTables(String catalogName);

    List<QualifiedTableName> listTables(String catalogName, String schemaName);

    List<TableColumn> listTableColumns(String catalogName);

    List<TableColumn> listTableColumns(String catalogName, String schemaName);

    List<TableColumn> listTableColumns(String catalogName, String schemaName, String tableName);

    List<String> listTablePartitionKeys(String catalogName, String schemaName, String tableName);

    List<Map<String, String>> listTablePartitionValues(String catalogName, String schemaName, String tableName);

    void createTable(TableMetadata table);

    QualifiedTableName getTableName(TableHandle tableHandle);

    TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle);
}
