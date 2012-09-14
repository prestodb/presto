package com.facebook.presto.block.position;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public class PositionsBlock
        implements TupleStream
{
    private final List<Range> ranges;
    private final Range totalRange;

    public PositionsBlock(Range... ranges)
    {
        this(asList(ranges));
    }

    public PositionsBlock(List<Range> ranges)
    {
        checkNotNull(ranges, "ranges is null");
        checkArgument(!ranges.isEmpty(), "ranges is empty");

        this.ranges = ImmutableList.copyOf(ranges);

        // verify ranges are not overlapping
        Range previousRange = ranges.get(0);
        for (int index = 1; index < ranges.size(); index++) {
            Range currentRange = ranges.get(index);
            checkArgument(!currentRange.overlaps(previousRange), "Ranges are overlapping");
            previousRange = currentRange;
        }

        this.totalRange = Range.create(ranges.get(0).getStart(), ranges.get(ranges.size() - 1).getEnd());
    }

    public int getCount()
    {
        return ranges.size();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.EMPTY_TUPLE_INFO;
    }

    @Override
    public Range getRange()
    {
        return totalRange;
    }

    @Override
    public Cursor cursor()
    {
        return new RangePositionBlockCursor(ranges, totalRange);
    }


    public static class RangePositionBlockCursor
            implements Cursor
    {
        private final List<Range> ranges;
        private final Range totalRange;
        private int index = -1;
        private long position = -1;

        public RangePositionBlockCursor(List<Range> ranges, Range totalRange)
        {
            this.ranges = ranges;
            this.totalRange = totalRange;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return TupleInfo.EMPTY_TUPLE_INFO;
        }

        @Override
        public Range getRange()
        {
            return totalRange;
        }

        @Override
        public boolean isValid()
        {
            return totalRange.contains(position);
        }

        @Override
        public boolean isFinished()
        {
            return position > totalRange.getEnd();
        }

        @Override
        public boolean advanceNextValue()
        {
            if (position >= totalRange.getEnd()) {
                return false;
            }

            if (index >= 0 && position < ranges.get(index).getEnd()) {
                position++;
            }
            else {
                index++;
                position = ranges.get(index).getStart();
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
            Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

            if (newPosition > totalRange.getEnd()) {
                index = ranges.size();
                position = newPosition;
                return false;
            }

            for (int i = index; i < ranges.size(); i++) {
                if (newPosition <= ranges.get(i).getEnd()) {
                    index = i;
                    position = Math.max(newPosition, ranges.get(i).getStart());
                    return true;
                }
            }
            // this should never happen
            throw new IllegalStateException("Invalid position");
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
