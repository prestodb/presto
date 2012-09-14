package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class RunLengthEncodedBlock
        implements Block
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

    @Override
    public int getCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
    }

    public Tuple getSingleValue()
    {
        return value;
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
    public BlockCursor blockCursor()
    {
        return new RunLengthEncodedBlockCursor(value, range);
    }

    public static final class RunLengthEncodedBlockCursor implements BlockCursor
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
        public Range getRange()
        {
            return range;
        }

        @Override
        public boolean advanceToNextValue()
        {
            position = Long.MAX_VALUE;
            return false;
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (position >= range.getEnd()) {
                position = Long.MAX_VALUE;
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
        public boolean advanceToPosition(long newPosition)
        {
            Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

            if (newPosition > range.getEnd()) {
                position = Long.MAX_VALUE;
                return false;
            }

            this.position = newPosition;
            return true;
        }

        @Override
        public Tuple getTuple()
        {
            return value;
        }

        @Override
        public long getLong(int field)
        {
            return value.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            return value.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return value.getSlice(field);
        }

        @Override
        public long getPosition()
        {
            return position;
        }

        @Override
        public long getValuePositionEnd()
        {
            return range.getEnd();
        }

        @Override
        public boolean tupleEquals(Tuple value)
        {
            return this.value.equals(value);
        }
    }
}
