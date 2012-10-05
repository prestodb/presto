package com.facebook.presto.aggregation;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

/**
 * A cursor that enumerates longs up to a max
 */
public class LongSequenceCursor
    implements Cursor
{
    private final long max;

    private long current = -1;

    public LongSequenceCursor(long max)
    {
        this.max = max;
    }

    public long getMax()
    {
        return max;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public Range getRange()
    {
        return new Range(0, max);
    }

    @Override
    public boolean isValid()
    {
        return current >= 0 && current <= max;
    }

    @Override
    public boolean isFinished()
    {
        return current > max;
    }

    @Override
    public boolean advanceNextValue()
    {
        current++;
        return !isFinished();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return advanceNextValue();
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        Preconditions.checkArgument(position >= current, "Can't advance backwards");
        current = position;

        return !isFinished();
    }

    @Override
    public Tuple getTuple()
    {
        return getTupleInfo().builder()
                .append(current)
                .build();
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkArgument(field == 0, "Tuple has only one field (0)");
        Preconditions.checkState(isValid(), "Cursor is not valid");
        return current;
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException("Cursor can only produce LONG");
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException("Cursor can only produce LONG");
    }

    @Override
    public long getPosition()
    {
        return current;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return current;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        return current == value.getLong(0);
    }
}
