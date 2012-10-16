package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class ColumnMappingTupleStream
        implements TupleStream
{
    private final TupleStream delegate;
    private final int[] selectedColumns;
    private final TupleInfo tupleInfo;

    public ColumnMappingTupleStream(TupleStream delegate, int... selectedColumns)
    {
        this(delegate, Ints.asList(selectedColumns));
    }

    public ColumnMappingTupleStream(TupleStream delegate, List<Integer> selectedColumnList)
    {
        checkNotNull(delegate, "delegate is null");
        checkNotNull(selectedColumnList, "selectedColumns is null");
        checkArgument(!selectedColumnList.isEmpty(), "must select at least one column");

        this.delegate = delegate;
        this.selectedColumns = new int[selectedColumnList.size()];
        for (int index = 0; index < selectedColumnList.size(); index++) {
            selectedColumns[index] = checkPositionIndex(selectedColumnList.get(index), delegate.getTupleInfo().getFieldCount(), "invalid column map");
        }

        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();
        for (Integer column : selectedColumns) {
            types.add(delegate.getTupleInfo().getTypes().get(column));
        }
        tupleInfo = new TupleInfo(types.build());
    }

    public static ColumnMappingTupleStream map(TupleStream blockStream, Integer... columns)
    {
        checkNotNull(blockStream, "blockStream is null");
        checkNotNull(columns, "columns is null");
        return new ColumnMappingTupleStream(blockStream, Arrays.asList(columns));
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return delegate.getRange();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        checkNotNull(session, "session is null");
        return new ColumnMappingCursor(delegate.cursor(session), selectedColumns);
    }
}
