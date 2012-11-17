package com.facebook.presto.split;

import com.facebook.presto.ingest.ImportPartition;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.ImportMetadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportDataStreamProvider
        implements DataStreamProvider
{
    private final ImportClientFactory importClientFactory;
    private final ImportMetadata metadata;

    @Inject
    public ImportDataStreamProvider(ImportClientFactory importClientFactory, ImportMetadata metadata)
    {
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        ImportSplit importSplit = (ImportSplit) split;
        ImportClient client = importClientFactory.getClient(importSplit.getSourceName());

        ImmutableList.Builder<RecordProjection> builder = ImmutableList.builder();
        for (int i = 0; i < columns.size(); i++) {
            ImportColumnHandle importColumn = (ImportColumnHandle) columns.get(i);
            // TODO: under the current implementation, this will be very slow as import metadata will fetch the full table metadata on each call. maybe add caching
            ColumnMetadata columnMetadata = metadata.getColumn(importColumn);
            builder.add(RecordProjections.createProjection(i, columnMetadata.getType()));
        }

        PartitionChunk partitionChunk = client.deserializePartitionChunk(importSplit.getSerializedChunk().getBytes());
        ImportPartition importPartition = new ImportPartition(client, partitionChunk, convertToColumnNames(columns));
        return new RecordProjectOperator(importPartition, builder.build());
    }

    private Iterable<String> convertToColumnNames(Iterable<ColumnHandle> columnHandles){
        return Iterables.transform(columnHandles, new Function<ColumnHandle, String>()
        {
            @Override
            public String apply(ColumnHandle columnHandle)
            {
                ImportColumnHandle importColumn = (ImportColumnHandle) columnHandle;
                return importColumn.getColumnName();
            }
        });
    }
}
