package com.facebook.presto.split;

import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.ImportColumnHandle.columnHandleGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class ImportDataStreamProvider
        implements DataStreamProvider
{
    private final ImportClientManager importClientManager;

    @Inject
    public ImportDataStreamProvider(ImportClientManager importClientManager)
    {
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
    }

    @Override
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof ImportSplit, "Split must be of type ImportSplit, not %s", split.getClass().getName());
        assert split instanceof ImportSplit; // // IDEA-60343
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        ImportSplit importSplit = (ImportSplit) split;
        ImportClient client = importClientManager.getClient(importSplit.getSourceName());

        List<ColumnHandle> columnHandles = ImmutableList.copyOf(transform(columns, columnHandleGetter()));

        PartitionChunk partitionChunk = client.deserializePartitionChunk(importSplit.getSerializedChunk().getBytes());
        return new RecordProjectOperator(client.getRecords(partitionChunk, columnHandles));
    }
}
