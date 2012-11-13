package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;

public class UncompressedBlock
        implements Block
{
    private final int positionCount;
    private final TupleInfo tupleInfo;
    private final Slice slice;
    private final int rawOffset;

    // The position this block starts in the underlying storage block
    private final int rawPositionOffset;
    private final int rawPositionCount;

    public UncompressedBlock(int positionCount, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.rawOffset = 0;
        this.positionCount = positionCount;
        this.rawPositionOffset = 0;
        this.rawPositionCount = positionCount;
    }

    public UncompressedBlock(int positionCount, TupleInfo tupleInfo, Slice slice, int rawOffset, int rawPositionOffset, int rawPositionCount)
    {
        this.positionCount = positionCount;
        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.rawOffset = rawOffset;
        this.rawPositionOffset = rawPositionOffset;
        this.rawPositionCount = rawPositionCount;
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

    public int getPositionCount()
    {
        return positionCount;
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
    public int getRawPositionCount()
    {
        return rawPositionCount;
    }

    @Override
    public Block createViewPort(int rawPosition, int length)
    {
        Preconditions.checkPositionIndexes(rawPosition, rawPosition + length, rawPositionCount);
        int rawOffset = getPositionRawOffset(rawPosition);
        return new UncompressedBlock(length, tupleInfo, slice, rawOffset, rawPosition, rawPositionCount);
    }

    private int getPositionRawOffset(long newPosition)
    {
        // optimizations for single field tuples
        if (tupleInfo.getFieldCount() == 1) {
            Type type = tupleInfo.getTypes().get(0);
            if (type == Type.FIXED_INT_64 || type == Type.DOUBLE) {
                return (int) ((SIZE_OF_LONG + SIZE_OF_BYTE) * newPosition);
            }
            if (type == Type.VARIABLE_BINARY) {
                long position;
                int offset;
                if (newPosition >= rawPositionOffset) {
                    position = rawPositionOffset;
                    offset = rawOffset;
                }
                else {
                    position = 0;
                    offset = 0;
                }

                int size = slice.getInt(offset + SIZE_OF_BYTE);
                while (position < newPosition) {
                    position++;
                    offset += size;
                    size = slice.getInt(offset + SIZE_OF_BYTE);
                }
                return offset;
            }
        }

        // general tuple
        long position;
        int offset;
        if (newPosition >= rawPositionOffset) {
            position = rawPositionOffset;
            offset = rawOffset;
        }
        else {
            position = 0;
            offset = 0;
        }

        int size = tupleInfo.size(slice, offset);
        while (position < newPosition) {
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
                .add("positionCount", positionCount)
                .add("tupleInfo", tupleInfo)
                .add("rawPositionOffset", rawPositionOffset)
                .add("rawPositionCount", rawPositionCount)
                .add("rawOffset", rawOffset)
                .add("slice", slice)
                .toString();
    }
}
