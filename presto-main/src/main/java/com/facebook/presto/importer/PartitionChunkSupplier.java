package com.facebook.presto.importer;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.split.ImportClientFactory;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.runWithRetryUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;

class PartitionChunkSupplier
        implements Supplier<List<SerializedPartitionChunk>>
{
    private final ImportClientFactory importClientFactory;
    private final String sourceName;
    private final String databaseName;
    private final String tableName;
    private final String partitionName;

    public PartitionChunkSupplier(ImportClientFactory importClientFactory, String sourceName, String databaseName, String tableName, String partitionName)
    {
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
        this.sourceName = checkNotNull(sourceName, "sourceName is null");
        this.databaseName = checkNotNull(databaseName, "databaseName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionName = checkNotNull(partitionName, "partitionName is null");
    }

    @Override
    public List<SerializedPartitionChunk> get()
    {
        List<PartitionChunk> chunks = runWithRetryUnchecked(new Callable<List<PartitionChunk>>()
        {
            @Override
            public List<PartitionChunk> call()
                    throws Exception
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                return importClient.getPartitionChunks(databaseName, tableName, partitionName);
            }
        });

        ImportClient importClient = importClientFactory.getClient(sourceName);
        ImmutableList.Builder<SerializedPartitionChunk> serialized = ImmutableList.builder();
        for (PartitionChunk chunk : chunks) {
            serialized.add(SerializedPartitionChunk.create(importClient, chunk));
        }
        return serialized.build();
    }
}
