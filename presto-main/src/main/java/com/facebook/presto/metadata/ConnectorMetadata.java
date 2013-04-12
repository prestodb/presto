package com.facebook.presto.metadata;

import java.util.List;
import java.util.Map;

public interface ConnectorMetadata
{
    int priority();

    boolean canHandle(TableHandle tableHandle);

    boolean canHandle(QualifiedTablePrefix prefix);

    List<String> listSchemaNames(String catalogName);

    TableMetadata getTable(QualifiedTableName tableName);

    List<QualifiedTableName> listTables(QualifiedTablePrefix prefix);

    List<TableColumn> listTableColumns(QualifiedTablePrefix prefix);

    List<String> listTablePartitionKeys(QualifiedTableName tableName);

    List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix);

    void createTable(TableMetadata table);

    void dropTable(TableMetadata table);

    QualifiedTableName getTableName(TableHandle tableHandle);

    TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle);
}
