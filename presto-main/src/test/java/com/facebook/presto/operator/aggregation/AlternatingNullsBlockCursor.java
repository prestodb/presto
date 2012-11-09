package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.Tuples;
import com.facebook.presto.util.Range;

/**
 * A wrapper that inserts a null in every other position
 */
public class AlternatingNullsBlockCursor
        implements BlockCursor
{
    private final BlockCursor delegate;
    private final Range range;
    private final Tuple nullTuple;
    private int index = -1;

    public AlternatingNullsBlockCursor(BlockCursor delegate)
    {
        this.delegate = delegate;
        range = new Range(delegate.getRange().getStart(), delegate.getRange().length() * 2);
        nullTuple = Tuples.nullTuple(this.delegate.getTupleInfo());
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return delegate.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public boolean isValid()
    {
        return index > 0 && delegate.isValid();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public boolean advanceNextValue()
    {
        return advanceNextPosition();
    }

    @Override
    public boolean advanceNextPosition()
    {
        index++;
        return isNullPosition() || delegate.advanceNextPosition();
    }

    private boolean isNullPosition()
    {
        return index % 2 == 0;
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Tuple getTuple()
    {
        if (isNullPosition()) {
            return nullTuple;
        }
        return delegate.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        if (isNullPosition()) {
            return 0;
        }
        return delegate.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        if (isNullPosition()) {
            return 0;
        }
        return delegate.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        if (isNullPosition()) {
            return Slices.EMPTY_SLICE;
        }
        return delegate.getSlice(field);
    }

    @Override
    public boolean isNull(int field)
    {
        return isNullPosition() || delegate.isNull(field);
    }

    @Override
    public long getPosition()
    {
        return delegate.getRange().getStart() + index;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        if (isNullPosition()) {
            return nullTuple.equals(value);
        }
        return delegate.currentTupleEquals(value);
    }
}
