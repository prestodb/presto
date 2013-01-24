package com.facebook.presto.spi;


import java.util.List;

/**
 * This wants to be part of the SPI at some point.
 */
public interface MetadataCache
{
    List<String> getDatabaseNames(ImportClient backingClient);

    List<String> getTableNames(ImportClient backingClient, String databaseName)
            throws ObjectNotFoundException;

    List<SchemaField> getTableSchema(ImportClient backingClient, String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<SchemaField> getPartitionKeys(ImportClient backingClient, String databaseName, String tableName)
            throws ObjectNotFoundException;

/*
    List<PartitionInfo> getPartitions(String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<PartitionInfo> getPartitions(String databaseName, String tableName, Map<String, Object> filters)
            throws ObjectNotFoundException;

    List<String> getPartitionNames(String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
            throws ObjectNotFoundException;

    Iterable<List<PartitionChunk>> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
            throws ObjectNotFoundException;
*/
}
