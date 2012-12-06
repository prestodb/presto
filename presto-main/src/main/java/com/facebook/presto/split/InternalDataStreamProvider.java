package com.facebook.presto.split;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.InformationSchemaData;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalDataStreamProvider
        implements DataStreamProvider
{
    private final InformationSchemaData informationSchemaData;

    @Inject
    public InternalDataStreamProvider(InformationSchemaData informationSchemaData)
    {
        this.informationSchemaData = checkNotNull(informationSchemaData, "informationSchemaData is null");
    }

    @Override
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof InternalSplit, "Split must be of type %s, not %s", InternalSplit.class.getName(), split.getClass().getName());
        assert split instanceof InternalSplit; // // IDEA-60343

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        InternalTableHandle handle = ((InternalSplit) split).getTableHandle();

        InternalTable table = informationSchemaData.getInternalTable(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName());

        ImmutableList.Builder<BlockIterable> list = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof InternalColumnHandle, "column must be of type %s, not %s", InternalColumnHandle.class.getName(), column.getClass().getName());
            assert column instanceof InternalColumnHandle; // // IDEA-60343
            InternalColumnHandle internalColumn = (InternalColumnHandle) column;

            list.add(table.getColumn(internalColumn.getColumnIndex()));
        }
        return new AlignmentOperator(list.build());
    }
}
