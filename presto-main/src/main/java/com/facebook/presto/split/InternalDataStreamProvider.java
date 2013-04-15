package com.facebook.presto.split;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.DualTable;
import com.facebook.presto.metadata.InformationSchemaData;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.InternalTableHandle;
import com.facebook.presto.metadata.SystemTables;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.SystemTables.SYSTEM_SCHEMA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class InternalDataStreamProvider
        implements DataStreamProvider
{
    private final InformationSchemaData informationSchemaData;
    private final SystemTables systemTables;

    @Inject
    public InternalDataStreamProvider(InformationSchemaData informationSchemaData, SystemTables systemTables)
    {
        this.informationSchemaData = checkNotNull(informationSchemaData, "informationSchemaData is null");
        this.systemTables = checkNotNull(systemTables, "systemTables is null");
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
        Map<InternalColumnHandle, Object> filters = ((InternalSplit) split).getFilters();

        InternalTable table;
        if (handle.getTableName().getTableName().equals(DualTable.NAME)) {
            table = new DualTable().getInternalTable(handle.getTableName());
        }
        else if (handle.getTableName().getSchemaName().equals(INFORMATION_SCHEMA)) {
            table = informationSchemaData.getInternalTable(handle.getTableName(), filters);
        }
        else if (handle.getTableName().getSchemaName().equals(SYSTEM_SCHEMA)) {
            table = systemTables.getInternalTable(handle.getTableName());
        }
        else {
            throw new IllegalArgumentException(format("table does not exist: %s", handle.getTableName()));
        }

        ImmutableList.Builder<BlockIterable> list = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof InternalColumnHandle, "column must be of type %s, not %s", InternalColumnHandle.class.getName(), column.getClass().getName());
            assert column instanceof InternalColumnHandle; // // IDEA-60343
            InternalColumnHandle internalColumn = (InternalColumnHandle) column;

            list.add(table.getColumn(internalColumn.getColumnName()));
        }
        return new AlignmentOperator(list.build());
    }
}
