package com.facebook.presto.connector.dual;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DualDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private static final InternalTable DATA = InternalTable.builder(ImmutableList.of(DualMetadata.COLUMN_METADATA))
            .add(SINGLE_VARBINARY.builder().append("X").build())
            .build();

    @Inject
    public DualDataStreamProvider()
    {
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof DualSplit;
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        return new AlignmentOperator(operatorContext, createChannels(split, columns));
    }

    private List<BlockIterable> createChannels(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof DualSplit, "Split must be of type %s, not %s", DualSplit.class.getName(), split.getClass().getName());

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        ImmutableList.Builder<BlockIterable> list = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof DualColumnHandle, "column must be of type %s, not %s", DualColumnHandle.class.getName(), column.getClass().getName());
            DualColumnHandle dualColumn = (DualColumnHandle) column;

            list.add(DATA.getColumn(dualColumn.getColumnName()));
        }
        return list.build();
    }
}
