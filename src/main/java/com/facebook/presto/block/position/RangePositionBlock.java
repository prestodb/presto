package com.facebook.presto.block.position;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class RangePositionBlock
        implements Block
{
    private final Range range;

    public RangePositionBlock(Range range)
    {
        this.range = range;
    }

    @Override
    public int getCount()
    {
        return (int) range.length();
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("range", range)
                .toString();
    }

    @Override
    public Cursor blockCursor()
    {
        return new RangePositionBlockCursor(range);
    }

    public static class RangePositionBlockCursor
            implements Cursor
    {
        private final Range range;
        private long position = -1;

        public RangePositionBlockCursor(Range range)
        {
            this.range = range;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return TupleInfo.EMPTY_TUPLE_INFO;
        }

        @Override
        public Range getRange()
        {
            return range;
        }

        @Override
        public boolean isFinished()
        {
            return position > range.getEnd();
        }

        @Override
        public boolean advanceNextValue()
        {
            if (position >= range.getEnd()) {
                return false;
            }
            if (position < 0) {
                position = range.getStart();
            } else {
                position++;
            }
            return true;
        }

        @Override
        public boolean advanceNextPosition()
        {
            return advanceNextValue();
        }

        @Override
        public boolean advanceToPosition(long newPosition)
        {
            Preconditions.checkArgument(newPosition >= position, "Invalid position");
            if (newPosition > range.getEnd()) {
                position = newPosition;
                return false;
            } else {
                position = newPosition;
                return true;
            }
        }

        @Override
        public long getPosition()
        {
            Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
            return position;
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            return getPosition();
        }

        @Override
        public Tuple getTuple()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getSlice(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            throw new UnsupportedOperationException();
        }
    }
}
