package com.facebook.presto.spi;


import java.util.List;
import java.util.Map;

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

    List<PartitionInfo> getPartitions(ImportClient backingClient, String databaseName, String tableName)
            throws ObjectNotFoundException;

    List<String> getPartitionNames(ImportClient backingClient, String databaseName, String tableName)
            throws ObjectNotFoundException;

    /**
     * Internal Cache elements that can be exposed through JMX.
     */
    Map<String, Object> getJmxExposed();
}
