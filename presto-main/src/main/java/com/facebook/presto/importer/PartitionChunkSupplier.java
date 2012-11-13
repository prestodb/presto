package com.facebook.presto.importer;

import com.facebook.presto.hive.ImportClient;
import com.facebook.presto.hive.PartitionChunk;
import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

class PartitionChunkSupplier
        implements Supplier<List<SerializedPartitionChunk>>
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
    public List<SerializedPartitionChunk> get()
    {
        List<PartitionChunk> chunks = importClient.getPartitionChunks(databaseName, tableName, partitionName);
        ImmutableList.Builder<SerializedPartitionChunk> serialized = ImmutableList.builder();
        for (PartitionChunk chunk : chunks) {
            serialized.add(SerializedPartitionChunk.create(importClient, chunk));
        }
        return serialized.build();
    }
}
