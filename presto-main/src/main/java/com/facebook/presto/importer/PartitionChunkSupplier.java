package com.facebook.presto.importer;

import com.facebook.presto.hive.ImportClient;
import com.facebook.presto.hive.PartitionChunk;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

class PartitionChunkSupplier
        implements Supplier<List<byte[]>>
{
    private final ImportClient importClient;
    private final String databaseName;
    private final String tableName;
    private final String partitionName;

    public PartitionChunkSupplier(ImportClient importClient, String databaseName, String tableName, String partitionName)
    {
        this.importClient = checkNotNull(importClient, "importClient is null");
        this.databaseName = checkNotNull(databaseName, "databaseName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionName = checkNotNull(partitionName, "partitionName is null");
    }

    @Override
    public List<byte[]> get()
    {
        List<PartitionChunk> chunks = importClient.getPartitionChunks(databaseName, tableName, partitionName);
        ImmutableList.Builder<byte[]> serialized = ImmutableList.builder();
        for (PartitionChunk chunk : chunks) {
            serialized.add(importClient.serializePartitionChunk(chunk));
        }
        return serialized.build();
    }
}
