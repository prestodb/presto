package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

public class RangePositionBlock
        implements ValueBlock
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
    public boolean isSorted()
    {
        return false;
    }

    @Override
    public boolean isSingleValue()
    {
        return getCount() == 1;
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
                .add("range", range)
                .toString();
    }

    @Override
    public BlockCursor blockCursor()
    {
        return new RangePositionBlockCursor(range);
    }

    public static class RangePositionBlockCursor
            implements BlockCursor
    {
        private Range range;
        private long position = -1;

        public RangePositionBlockCursor(Range range)
        {
            this.range = range;
        }

        @Override
        public Range getRange()
        {
            return range;
        }

        @Override
        public void moveTo(BlockCursor newPosition)
        {
            RangePositionBlockCursor cursor = (RangePositionBlockCursor) newPosition;
            range = cursor.range;
            position = cursor.position;
        }

        @Override
        public boolean hasNextValue()
        {
            return position < range.getEnd();
        }

        @Override
        public void advanceNextValue()
        {
            if (!hasNextValue()) {
                throw new NoSuchElementException();
            }
            if (position < 0) {
                position = range.getStart();
            } else {
                position++;
            }
        }

        @Override
        public boolean hasNextPosition()
        {
            return hasNextValue();
        }

        @Override
        public void advanceNextPosition()
        {
            advanceNextValue();
        }

        @Override
        public void advanceToPosition(long newPosition)
        {
            Preconditions.checkArgument(range.contains(newPosition), "Invalid position");
            position = newPosition;
        }

        @Override
        public long getPosition()
        {
            Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
            return position;
        }

        @Override
        public long getValuePositionEnd()
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
        public Slice getSlice(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tupleEquals(Tuple value)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static Function<RangePositionBlock, Range> rangeGetter()
    {
        return new Function<RangePositionBlock, Range>()
        {
            @Override
            public Range apply(RangePositionBlock input)
            {
                return input.getRange();
            }
        };
    }
}
