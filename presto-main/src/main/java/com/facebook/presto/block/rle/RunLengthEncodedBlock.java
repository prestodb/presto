package com.facebook.presto.block.rle;

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

public class RunLengthEncodedBlock
        implements TupleStream
{
    private final Tuple value;
    private final Range range;

    public RunLengthEncodedBlock(Tuple value, Range range)
    {
        this.value = value;
        this.range = range;
    }

    public Tuple getValue()
    {
        return value;
    }

    public int getCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
    }

    public Tuple getSingleValue()
    {
        return value;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return value.getTupleInfo();
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
                .add("value", value)
                .add("range", range)
                .toString();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new RunLengthEncodedBlockCursor(value, range);
    }

    public static final class RunLengthEncodedBlockCursor implements Cursor
    {
        private final Tuple value;
        private final Range range;
        private long position = -1;

        public RunLengthEncodedBlockCursor(Tuple value, Range range)
        {
            this.value = value;
            this.range = range;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return value.getTupleInfo();
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
            position = Long.MAX_VALUE;
            return FINISHED;
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            if (position >= range.getEnd()) {
                position = Long.MAX_VALUE;
                return FINISHED;
            }

            if (position < 0) {
                position = range.getStart();
            } else {
                position++;
            }
            return SUCCESS;
        }

        @Override
        public AdvanceResult advanceToPosition(long newPosition)
        {
            if (newPosition > range.getEnd()) {
                position = Long.MAX_VALUE;
                return FINISHED;
            }

            Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

            this.position = newPosition;
            return SUCCESS;
        }

        @Override
        public Tuple getTuple()
        {
            Cursors.checkReadablePosition(this);
            return value;
        }

        @Override
        public long getLong(int field)
        {
            Cursors.checkReadablePosition(this);
            return value.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            Cursors.checkReadablePosition(this);
            return value.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            Cursors.checkReadablePosition(this);
            return value.getSlice(field);
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
            Cursors.checkReadablePosition(this);
            return range.getEnd();
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            Cursors.checkReadablePosition(this);
            return this.value.equals(value);
        }
    }
}
