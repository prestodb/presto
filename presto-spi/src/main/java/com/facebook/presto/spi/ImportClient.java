package com.facebook.presto.spi;

import java.util.List;
import java.util.Map;

public interface ImportClient
{
    List<String> listSchemaNames();

    TableHandle getTableHandle(SchemaTableName tableName);

    SchemaTableName getTableName(TableHandle tableHandle);

    SchemaTableMetadata getTableMetadata(TableHandle table);

    List<SchemaTableName> listTables(String schemaNameOrNull);

    ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName);

    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle);

    Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix);

    List<Map<String, String>> listTablePartitionValues(SchemaTablePrefix prefix);

    List<PartitionInfo> getPartitions(SchemaTableName table, Map<String, Object> filters);

    List<String> getPartitionNames(SchemaTableName tableName);

    Iterable<PartitionChunk> getPartitionChunks(SchemaTableName tableName, String partitionName, List<String> columns);

    Iterable<PartitionChunk> getPartitionChunks(SchemaTableName tableName, List<String> partitionNames, List<String> columns);

    RecordCursor getRecords(PartitionChunk partitionChunk);

    byte[] serializePartitionChunk(PartitionChunk partitionChunk);

    PartitionChunk deserializePartitionChunk(byte[] bytes);
}
