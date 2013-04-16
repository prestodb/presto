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

    List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings);

    Iterable<PartitionChunk> getPartitionChunks(List<Partition> partitions, List<ColumnHandle> columns);

    RecordCursor getRecords(PartitionChunk partitionChunk);

    byte[] serializePartitionChunk(PartitionChunk partitionChunk);

    PartitionChunk deserializePartitionChunk(byte[] bytes);
}
