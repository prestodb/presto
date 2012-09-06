package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.cursor.UncompressedBlockCursor;
import com.facebook.presto.block.cursor.UncompressedLongBlockCursor;
import com.facebook.presto.block.cursor.UncompressedSliceBlockCursor;
import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class UncompressedValueBlock
        implements ValueBlock
{
    private final Range range;
    private final TupleInfo info;
    private final Slice slice;

    public UncompressedValueBlock(Range range, TupleInfo info, Slice slice)
    {
        Preconditions.checkNotNull(range, "range is null");
        Preconditions.checkArgument(range.getStart() >= 0, "range start position is negative");
        Preconditions.checkNotNull(info, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.info = info;
        this.slice = slice;
        this.range = range;
    }

    public Slice getSlice()
    {
        return slice;
    }

    @Override
    public Optional<PositionBlock> selectPositions(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ValueBlock> selectPairs(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PositionBlock toPositionBlock()
    {
        return new RangePositionBlock(range);
    }

    /**
     * Build a new block with only the selected value positions
     */
    @Override
    public Optional<ValueBlock> filter(PositionBlock positions)
    {
        return MaskedValueBlock.maskBlock(this, positions);
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return new AbstractIterator<Tuple>()
        {
            private int currentOffset = 0;
            private long index = 0;

            @Override
            protected Tuple computeNext()
            {
                if (index >= getCount()) {
                    endOfData();
                    return null;
                }

                Slice currentPositionToEnd = slice.slice(currentOffset, slice.length() - currentOffset);

                int size = info.size(currentPositionToEnd);
                index++;
                currentOffset += size;

                Slice row = currentPositionToEnd.slice(0, size);
                return new Tuple(row, info);
            }
        };
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(new AbstractIterator<Pair>()
        {
            private int currentOffset = 0;
            private long index = 0;

            @Override
            protected Pair computeNext()
            {
                if (index >= getCount()) {
                    endOfData();
                    return null;
                }

                Slice currentPositionToEnd = slice.slice(currentOffset, slice.length() - currentOffset);

                int size = info.size(currentPositionToEnd);
                currentOffset += size;

                Slice row = currentPositionToEnd.slice(0, size);

                long position = index + range.getStart();
                index++;
                return new Pair(position, new Tuple(row, info));
            }
        });
    }

    @Override
    public int getCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
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
    public Tuple getSingleValue()
    {
        Preconditions.checkState(isSingleValue(), "Block contains more than one value");
        return iterator().next();
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return true;
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return range;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public BlockCursor blockCursor()
    {
        if (info.getFieldCount() == 1) {
            Type type = info.getTypes().get(0);
            if (type == Type.FIXED_INT_64) {
                return new UncompressedLongBlockCursor(this);
            }
            if (type == Type.VARIABLE_BINARY) {
                return new UncompressedSliceBlockCursor(this);
            }
        }
        return new UncompressedBlockCursor(info, this);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("range", range)
                .add("tupleInfo", info)
                .add("slice", slice)
                .toString();
    }
}
