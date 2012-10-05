package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
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
        Preconditions.checkNotNull(session, "session is null");
        return new ForwardingCursor(delegate.cursor(session))
        {
            @Override
            public TupleInfo getTupleInfo()
            {
                return tupleInfo;
            }

            @Override
            public Tuple getTuple()
            {
                // Materializes the column mapped tuple... SLOW!
                Tuple originalTuple = getDelegate().getTuple();
                TupleInfo.Builder builder = tupleInfo.builder();
                for (int column : selectedColumns) {
                    switch (originalTuple.getTupleInfo().getTypes().get(column)) {
                        case FIXED_INT_64:
                            builder.append(originalTuple.getLong(column));
                            break;
                        case DOUBLE:
                            builder.append(originalTuple.getDouble(column));
                            break;
                        case VARIABLE_BINARY:
                            builder.append(originalTuple.getSlice(column));
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + tupleInfo.getTypes().get(column));
                    }
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
                if (!tupleInfo.equals(value.getTupleInfo())) {
                    return false;
                }
                for (int index = 0; index < tupleInfo.getFieldCount(); index++) {
                    switch (tupleInfo.getTypes().get(index)) {
                        case FIXED_INT_64:
                            if (getDelegate().getLong(remapColumnIndex(index)) != value.getLong(index)) {
                                return false;
                            }
                            break;
                        case DOUBLE:
                            if (getDelegate().getDouble(remapColumnIndex(index)) != value.getDouble(index)) {
                                return false;
                            }
                            break;
                        case VARIABLE_BINARY:
                            if (!getDelegate().getSlice(remapColumnIndex(index) ).equals(value.getSlice(index))) {
                                return false;
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + tupleInfo.getTypes().get(index));
                    }
                }
                return true;
            }

            private int remapColumnIndex(int column)
            {
                checkPositionIndex(column, selectedColumns.length, "invalid column index");
                return selectedColumns[column];
            }
        };
}
}
