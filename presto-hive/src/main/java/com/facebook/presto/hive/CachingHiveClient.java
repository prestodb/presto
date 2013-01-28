package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.MetadataCache;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

public class CachingHiveClient
        implements ImportClient
{
    private final MetadataCache metadataCache;
    private final ImportClient backingClient;

    public CachingHiveClient(MetadataCache metadataCache, HiveClient hiveClient)
    {
        Preconditions.checkNotNull(metadataCache, "The metadataCache can not be null!");
        Preconditions.checkNotNull(hiveClient, "hiveClient is null");
        this.metadataCache = metadataCache;
        this.backingClient = hiveClient;
    }

    @Override
    public List<String> getDatabaseNames()
    {
        return metadataCache.getDatabaseNames(backingClient);
    }

    @Override
    public List<String> getTableNames(String databaseName)
            throws ObjectNotFoundException
    {
        return metadataCache.getTableNames(backingClient, databaseName);
    }

    @Override
    public List<SchemaField> getTableSchema(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        return metadataCache.getTableSchema(backingClient, databaseName, tableName);
    }

    @Override
    public List<SchemaField> getPartitionKeys(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        return metadataCache.getPartitionKeys(backingClient, databaseName, tableName);
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        return metadataCache.getPartitionNames(backingClient, databaseName, tableName);
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName, Map<String, Object> filters)
            throws ObjectNotFoundException
    {
        List<PartitionInfo> partitions = metadataCache.getPartitions(backingClient, databaseName, tableName);
        return ImmutableList.copyOf(Iterables.filter(partitions, HiveClient.partitionMatches(filters)));
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        return metadataCache.getPartitions(backingClient, databaseName, tableName);
    }

    @Override
    public List<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
            throws ObjectNotFoundException
    {
        return backingClient.getPartitionChunks(databaseName, tableName, partitionName, columns);
    }

    @Override
    public Iterable<List<PartitionChunk>> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
            throws ObjectNotFoundException
    {
        return backingClient.getPartitionChunks(databaseName, tableName, partitionNames, columns);
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        return backingClient.getRecords(partitionChunk);
    }

    @Override
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        return backingClient.serializePartitionChunk(partitionChunk);
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        return backingClient.deserializePartitionChunk(bytes);
    }
}
