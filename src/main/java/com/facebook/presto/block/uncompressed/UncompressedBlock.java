package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class UncompressedBlock
        implements TupleStream
{
    private final Range range;
    private final TupleInfo tupleInfo;
    private final Slice slice;

    public UncompressedBlock(Range range, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkNotNull(range, "range is null");
        Preconditions.checkArgument(range.getStart() >= 0, "range start position is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.range = range;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public int getCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public Cursor cursor()
    {
        if (tupleInfo.getFieldCount() == 1) {
            Type type = tupleInfo.getTypes().get(0);
            if (type == Type.FIXED_INT_64) {
                return new UncompressedLongBlockCursor(this);
            }
            if (type == Type.DOUBLE) {
                return new UncompressedDoubleBlockCursor(this);
            }
            if (type == Type.VARIABLE_BINARY) {
                return new UncompressedSliceBlockCursor(this);
            }
        }
        return new UncompressedBlockCursor(this);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("range", range)
                .add("tupleInfo", tupleInfo)
                .add("slice", slice)
                .toString();
    }
}
