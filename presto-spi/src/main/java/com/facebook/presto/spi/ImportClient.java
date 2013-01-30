package com.facebook.presto.spi;

import java.util.List;
import java.util.Map;

public interface ImportClient
{
    List<String> getDatabaseNames();

    List<String> getTableNames(String databaseName)
            throws ObjectNotFoundException;

    List<SchemaField> getTableSchema(String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<SchemaField> getPartitionKeys(String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<PartitionInfo> getPartitions(String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<PartitionInfo> getPartitions(String databaseName, String tableName, Map<String, Object> filters)
            throws ObjectNotFoundException;

    List<String> getPartitionNames(String databaseName, String tableName)
            throws ObjectNotFoundException;

    Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
            throws ObjectNotFoundException;

    Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
            throws ObjectNotFoundException;

    RecordCursor getRecords(PartitionChunk partitionChunk);

    byte[] serializePartitionChunk(PartitionChunk partitionChunk);

    PartitionChunk deserializePartitionChunk(byte[] bytes);
}
