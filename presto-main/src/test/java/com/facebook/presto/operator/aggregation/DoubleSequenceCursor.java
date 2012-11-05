package com.facebook.presto.operator.aggregation;

import com.facebook.presto.util.Range;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;

/**
 * A cursor that enumerates integral doubles up to a max
 */
public class DoubleSequenceCursor
    implements BlockCursor
{
    private final long max;

    private long current = -1;

    public DoubleSequenceCursor(long max)
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
        return TupleInfo.SINGLE_DOUBLE;
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
        return current >= max;
    }

    private void checkReadablePosition()
    {
        if (isFinished()) {
            throw new NoSuchElementException("already finished");
        }
        checkState(isValid(), "cursor not yet advanced");
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
        checkReadablePosition();
        return getTupleInfo().builder()
                .append(current)
                .build();
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException("Cursor can only produce LONG");
    }

    @Override
    public double getDouble(int field)
    {

        Preconditions.checkArgument(field == 0, "Tuple has only one field (0)");
        checkReadablePosition();
        return current;
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException("Cursor can only produce LONG");
    }

    @Override
    public long getPosition()
    {
        checkReadablePosition();
        return current;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        checkReadablePosition();
        return current;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        return current == value.getDouble(0);
    }
}
