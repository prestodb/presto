package com.facebook.presto.spi;


import java.util.List;
import java.util.Map;

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
     * @TODO This is not a great API to have. Revisit that once we have better ways to expose stats.
     */
    Map<String, Object> getMetadataCacheStats();
}
