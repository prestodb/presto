package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class ProjectionTupleStream
        implements TupleStream
{
    private final TupleStream delegate;
    private final int[] selectedColumns;
    private final TupleInfo tupleInfo;

    public ProjectionTupleStream(TupleStream delegate, int... selectedColumns)
    {
        this(delegate, Ints.asList(selectedColumns));
    }

    public ProjectionTupleStream(TupleStream delegate, List<Integer> selectedColumnList)
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

    public static ProjectionTupleStream project(TupleStream blockStream, Integer... columns)
    {
        checkNotNull(blockStream, "blockStream is null");
        checkNotNull(columns, "columns is null");
        return new ProjectionTupleStream(blockStream, Arrays.asList(columns));
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
        return new ProjectionCursor(delegate.cursor(session), selectedColumns);
    }
}
