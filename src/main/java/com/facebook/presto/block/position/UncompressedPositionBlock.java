package com.facebook.presto.block.position;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.primitives.Longs.asList;

public class UncompressedPositionBlock
        implements Block
{
    private final List<Long> positions;
    private final Range range;

    public UncompressedPositionBlock(long... positions)
    {
        this(asList(positions));
    }

    public UncompressedPositionBlock(List<Long> positions)
    {
        checkNotNull(positions, "positions is null");
        checkArgument(!positions.isEmpty(), "positions is empty");

        this.positions = ImmutableList.copyOf(positions);

        this.range = Range.create(positions.get(0), positions.get(positions.size() - 1));
    }

    @Override
    public int getCount()
    {
        return positions.size();
    }

    @Override
    public boolean isSorted()
    {
        return true;
    }

    @Override
    public boolean isSingleValue()
    {
        return positions.size() == 1;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return false;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public BlockCursor blockCursor()
    {
        return new UncompressedPositionBlockCursor(positions, range);
    }

    public static class UncompressedPositionBlockCursor
            implements BlockCursor
    {
        private final List<Long> positions;
        private final Range range;
        private int index = -1;

        public UncompressedPositionBlockCursor(List<Long> positions, Range range)
        {
            this.positions = positions;
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
            if (index >= positions.size() - 1) {
                return false;
            }
            index++;
            return true;
        }

        @Override
        public boolean advanceNextPosition()
        {
            return advanceToNextValue();
        }

        @Override
        public boolean advanceToPosition(long newPosition)
        {
            Preconditions.checkArgument(index < 0 && newPosition >= 0 || newPosition >= positions.get(index), "Can't advance backwards");

            if (index < 0) {
                index = 0;
            }

            while (index < positions.size() && newPosition > positions.get(index)) {
                index++;
            }

            return index < positions.size();
        }

        @Override
        public long getPosition()
        {
            Preconditions.checkState(index >= 0, "Need to call advanceNext() first");

            if (index >=  positions.size()) {
                throw new NoSuchElementException();
            }

            return positions.get(index);
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
        public boolean tupleEquals(Tuple value)
        {
            throw new UnsupportedOperationException();
        }
    }
}
