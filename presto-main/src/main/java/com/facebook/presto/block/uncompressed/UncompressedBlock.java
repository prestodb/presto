package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.slice.SizeOf;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.Range;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class UncompressedBlock
        implements Block
{
    private final Range range;
    private final TupleInfo tupleInfo;
    private final Slice slice;
    private final int rawOffset;
    private final Range rawRange;

    public UncompressedBlock(Range range, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkNotNull(range, "range is null");
        Preconditions.checkArgument(range.getStart() >= 0, "range start position is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.range = range;
        this.rawOffset = 0;
        this.rawRange = range;
    }

    private UncompressedBlock(Range range, TupleInfo tupleInfo, Slice slice, int rawOffset, Range rawRange)
    {
        this.range = range;
        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.rawOffset = rawOffset;
        this.rawRange = rawRange;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public int getRawOffset()
    {
        return rawOffset;
    }

    public Range getRawRange()
    {
        return rawRange;
    }

    public int getPositionCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public BlockCursor cursor()
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
    public UncompressedBlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(tupleInfo);
    }

    @Override
    public UncompressedBlock createViewPort(Range viewPortRange)
    {
        Preconditions.checkArgument(rawRange.contains(viewPortRange), "view port range is must be within the range range of this block");
        int rawPositionOffset = getPositionRawOffset(viewPortRange.getStart());
        return new UncompressedBlock(viewPortRange, tupleInfo, slice, rawPositionOffset, rawRange);
    }

    private int getPositionRawOffset(long start)
    {
        // optimizations for single field tuples
        if (tupleInfo.getFieldCount() == 1) {
            Type type = tupleInfo.getTypes().get(0);
            if (type == Type.FIXED_INT_64 || type == Type.DOUBLE) {
                return (int) (SizeOf.SIZE_OF_LONG * (start - rawRange.getStart()));
            }
            if (type == Type.VARIABLE_BINARY) {
                long position;
                int offset;
                if (start >= range.getStart()) {
                    position = range.getStart();
                    offset = rawOffset;
                }
                else {
                    position = rawRange.getStart();
                    offset = 0;
                }

                int size = slice.getInt(offset);
                while (position < start) {
                    position++;
                    offset += size;
                    size = slice.getInt(offset);
                }
                return offset;
            }
        }

        // general tuple
        long position;
        int offset;
        if (start >= range.getStart()) {
            position = range.getStart();
            offset = rawOffset;
        }
        else {
            position = rawRange.getStart();
            offset = 0;
        }

        int size = tupleInfo.size(slice, offset);
        while (position < start) {
            position++;
            offset += size;
            size = tupleInfo.size(slice, offset);
        }
        return offset;
    }


    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("range", range)
                .add("tupleInfo", tupleInfo)
                .add("rawOffset", rawOffset)
                .add("slice", slice)
                .toString();
    }
}
