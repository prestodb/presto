package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.Split;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchDataStreamProvider
    implements DataStreamProvider
{
    private final TpchBlocksProvider tpchBlocksProvider;

    public TpchDataStreamProvider(TpchBlocksProvider tpchBlocksProvider)
    {
        this.tpchBlocksProvider = Preconditions.checkNotNull(tpchBlocksProvider, "tpchBlocksProvider is null");
    }

    @Override
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof TpchSplit, "Split must be a tpch split!");

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        TpchSplit tpchSplit = (TpchSplit) split;

        ImmutableList.Builder<BlockIterable> builder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof  TpchColumnHandle, "column must be of type TpchColumnHandle, not %s", column.getClass().getName());
            builder.add(tpchBlocksProvider.getBlocks(tpchSplit.getTableHandle(),
                    (TpchColumnHandle) column,
                    tpchSplit.getPartNumber(),
                    tpchSplit.getTotalParts(),
                    BlocksFileEncoding.RAW));
        }
        return new AlignmentOperator(builder.build());
    }
}
