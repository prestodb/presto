package com.facebook.presto.split;

import com.facebook.presto.ingest.ImportPartition;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportDataStreamProvider
        implements DataStreamProvider
{
    private final ImportClientFactory importClientFactory;

    @Inject
    public ImportDataStreamProvider(ImportClientFactory importClientFactory)
    {
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
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
        ImportClient client = importClientFactory.getClient(importSplit.getSourceName());

        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columns) {
            checkArgument(columnHandle instanceof ImportColumnHandle, "columnHandle must be of type ImportColumnHandle, not %s", columnHandle.getClass().getName());
            assert columnHandle instanceof ImportColumnHandle; // // IDEA-60343

            ImportColumnHandle importColumn = (ImportColumnHandle) columnHandle;
            builder.add(importColumn.getColumnType());
        }

        PartitionChunk partitionChunk = client.deserializePartitionChunk(importSplit.getSerializedChunk().getBytes());
        DataSize partitionSize = new DataSize(partitionChunk.getLength(), Unit.BYTE);
        ImportPartition importPartition = new ImportPartition(client, partitionChunk);
        return new RecordProjectOperator(importPartition, partitionSize, builder.build());
    }
}
