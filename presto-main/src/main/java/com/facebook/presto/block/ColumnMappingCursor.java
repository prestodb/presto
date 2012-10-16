package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.google.common.base.Preconditions.*;

public class ColumnMappingCursor
        extends ForwardingCursor
{
    private final int[] selectedColumns;
    private final TupleInfo tupleInfo;

    public ColumnMappingCursor(Cursor delegate, int... selectedColumns)
    {
        this(delegate, Ints.asList(selectedColumns));
    }

    public ColumnMappingCursor(Cursor delegate, List<Integer> selectedColumnList)
    {
        super(checkNotNull(delegate, "delegate is null"));

        checkNotNull(selectedColumnList, "selectedColumns is null");
        checkArgument(!selectedColumnList.isEmpty(), "must select at least one column");

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

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Tuple getTuple()
    {
        TupleInfo.Builder builder = tupleInfo.builder();
        for (int column : selectedColumns) {
            Cursors.appendCurrentTupleFieldToTupleBuilder(getDelegate(), column, builder);
        }
        return builder.build();
    }

    @Override
    public long getLong(int field)
    {
        return getDelegate().getLong(remapColumnIndex(field));
    }

    @Override
    public double getDouble(int field)
    {
        return getDelegate().getDouble(remapColumnIndex(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        return getDelegate().getSlice(remapColumnIndex(field));
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkNotNull(value, "value is null");
        Cursors.checkReadablePosition(this);
        return Cursors.currentTupleEquals(this, value);
    }

    private int remapColumnIndex(int column)
    {
        checkPositionIndex(column, selectedColumns.length, "invalid column index");
        return selectedColumns[column];
    }
}
