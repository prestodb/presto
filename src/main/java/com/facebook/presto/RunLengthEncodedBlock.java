package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class RunLengthEncodedBlock
        implements ValueBlock
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

    @Override
    public boolean isSorted()
    {
        return true;
    }

    @Override
    public boolean isSingleValue()
    {
        return true;
    }

    public Tuple getSingleValue()
    {
        return value;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return true;
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
        private Tuple value;
        private Range range;
        private long position;

        public RunLengthEncodedBlockCursor(Tuple value, Range range)
        {
            this.value = value;
            this.range = range;
            position = -1;
        }

        @Override
        public Range getRange()
        {
            return range;
        }

        @Override
        public void moveTo(BlockCursor newPosition)
        {
            RunLengthEncodedBlockCursor other = (RunLengthEncodedBlockCursor) newPosition;
            value = other.value;
            range = other.range;
            position = other.position;
        }

        @Override
        public boolean advanceToNextValue()
        {
            return false;
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (position > range.getEnd()) {
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
                this.position = newPosition;
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
