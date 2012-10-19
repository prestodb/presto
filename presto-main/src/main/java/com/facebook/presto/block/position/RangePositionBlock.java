package com.facebook.presto.block.position;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class RangePositionBlock
        implements TupleStream
{
    private final Range range;

    public RangePositionBlock(Range range)
    {
        this.range = range;
    }

    public int getCount()
    {
        return (int) range.length();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.EMPTY;
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
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
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
            return TupleInfo.EMPTY;
        }

        @Override
        public Range getRange()
        {
            return range;
        }

        @Override
        public boolean isValid()
        {
            return range.contains(position);
        }

        @Override
        public boolean isFinished()
        {
            return position > range.getEnd();
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            if (position >= range.getEnd()) {
                return FINISHED;
            }
            if (position < 0) {
                position = range.getStart();
            }
            else {
                position++;
            }
            return SUCCESS;
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            return advanceNextValue();
        }

        @Override
        public AdvanceResult advanceToPosition(long newPosition)
        {
            if (newPosition > range.getEnd()) {
                position = Long.MAX_VALUE;
                return FINISHED;
            }

            Preconditions.checkArgument(newPosition >= position, "Invalid position");
            position = newPosition;
            return SUCCESS;
        }

        @Override
        public long getPosition()
        {
            Cursors.checkReadablePosition(this);
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
