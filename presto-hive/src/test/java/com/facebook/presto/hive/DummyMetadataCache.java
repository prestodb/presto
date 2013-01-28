package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.MetadataCache;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.SchemaField;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DummyMetadataCache
        implements MetadataCache
{
    @Override
    public List<String> getDatabaseNames(ImportClient backingClient)
    {
        return backingClient.getDatabaseNames();
    }

    @Override
    public List<String> getTableNames(ImportClient backingClient, String databaseName) throws ObjectNotFoundException
    {
        return backingClient.getTableNames(databaseName);
    }

    @Override
    public List<SchemaField> getTableSchema(ImportClient backingClient, String databaseName, String tableName) throws ObjectNotFoundException
    {
        return backingClient.getTableSchema(databaseName, tableName);
    }

    @Override
    public List<SchemaField> getPartitionKeys(ImportClient backingClient, String databaseName, String tableName) throws ObjectNotFoundException
    {
        return backingClient.getPartitionKeys(databaseName, tableName);
    }

    @Override
    public List<PartitionInfo> getPartitions(ImportClient backingClient, String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        return backingClient.getPartitions(databaseName, tableName);
    }

    @Override
    public List<String> getPartitionNames(ImportClient backingClient, String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        return backingClient.getPartitionNames(databaseName, tableName);
    }

    @Override
    public Map<String, Object> getMetadataCacheStats()
    {
        return Collections.emptyMap();
    }
}
