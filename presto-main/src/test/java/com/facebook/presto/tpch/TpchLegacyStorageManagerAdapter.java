package com.facebook.presto.tpch;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.operator.Operator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.tpch.TpchSchema.columnHandle;
import static com.facebook.presto.tpch.TpchSchema.tableHandle;
import static com.google.common.base.Preconditions.checkArgument;

// TODO: get rid of this hack when ExecutionPlanner switches to DataStreamProvider!
public class TpchLegacyStorageManagerAdapter
    implements LegacyStorageManager
{
    private final TpchDataStreamProvider tpchDataStreamProvider;

    public TpchLegacyStorageManagerAdapter(TpchDataStreamProvider tpchDataStreamProvider)
    {
        this.tpchDataStreamProvider = Preconditions.checkNotNull(tpchDataStreamProvider, "tpchDataStreamProvider is null");
    }

    @Override
    public Operator getOperator(String databaseName, String tableName, List<Integer> fieldIndexes)
    {
        checkArgument(databaseName.equals(TpchSchema.SCHEMA_NAME));
        ImmutableList.Builder<ColumnHandle> builder = ImmutableList.builder();
        TpchTableHandle tableHandle = tableHandle(tableName);
        for (Integer fieldIndex : fieldIndexes) {
            builder.add(columnHandle(tableHandle, fieldIndex));
        }

        return tpchDataStreamProvider.createDataStream(new TpchSplit(tableHandle), builder.build());
    }
}
