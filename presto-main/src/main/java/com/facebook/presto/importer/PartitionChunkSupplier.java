package com.facebook.presto.importer;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.split.ImportClientManager;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;

class PartitionChunkSupplier
        implements Supplier<Iterable<SerializedPartitionChunk>>
{
    private final ImportClientManager importClientManager;
    private final String sourceName;
    private final String databaseName;
    private final String tableName;
    private final String partitionName;
    private final List<String> columns;

    public PartitionChunkSupplier(ImportClientManager importClientManager, String sourceName, String databaseName, String tableName, String partitionName, List<String> columns)
    {
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
        this.sourceName = checkNotNull(sourceName, "sourceName is null");
        this.databaseName = checkNotNull(databaseName, "databaseName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionName = checkNotNull(partitionName, "partitionName is null");
        this.columns = checkNotNull(columns, "columns is null");
    }

    @Override
    public Iterable<SerializedPartitionChunk> get()
    {
        Iterable<PartitionChunk> chunks = retry().stopOn(ObjectNotFoundException.class).runUnchecked(new Callable<Iterable<PartitionChunk>>()
        {
            @Override
            public Iterable<PartitionChunk> call()
                    throws Exception
            {
                ImportClient importClient = importClientManager.getClient(sourceName);
                return importClient.getPartitionChunks(databaseName, tableName, partitionName, columns);
            }
        });

        final ImportClient importClient = importClientManager.getClient(sourceName);
        return Iterables.transform(chunks, new Function<PartitionChunk, SerializedPartitionChunk>()
        {
            @Override
            public SerializedPartitionChunk apply(PartitionChunk chunk)
            {
                return SerializedPartitionChunk.create(importClient, chunk);
            }
        });
    }
}
