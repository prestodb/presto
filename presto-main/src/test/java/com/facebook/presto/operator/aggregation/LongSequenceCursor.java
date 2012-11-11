package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkState;

/**
 * A cursor that enumerates longs up to a max
 */
public class LongSequenceCursor
    implements BlockCursor
{
    private final int max;

    private int current = -1;

    public LongSequenceCursor(int max)
    {
        this.max = max;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public int getRemainingPositions()
    {
        return max - (current + 1);
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
        checkState(isValid(), "cursor is not valid");
    }

    @Override
    public boolean advanceNextPosition()
    {
        current++;
        return !isFinished();
    }

    @Override
    public boolean advanceToPosition(int position)
    {
        Preconditions.checkArgument(position >= current, "Can't advance backwards");
        current = position;

        return !isFinished();
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        throw new UnsupportedOperationException("No block form for " + getClass().getSimpleName());
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
        checkReadablePosition();
        Preconditions.checkArgument(field == 0, "Tuple has only one field (0)");
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
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public int getPosition()
    {
        checkReadablePosition();
        return current;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        return current == value.getLong(0);
    }

    @Override
    public int getRawOffset()
    {
        throw new UnsupportedOperationException();
    }
}
