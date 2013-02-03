package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public class UncompressedBlock
        implements Block
{
    private final int positionCount;
    private final TupleInfo tupleInfo;
    private final Slice slice;

    public UncompressedBlock(int positionCount, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.positionCount = positionCount;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public int getSliceOffset()
    {
        return 0;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(slice.length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        if (tupleInfo.getFieldCount() == 1) {
            Type type = tupleInfo.getTypes().get(0);
            if (type == Type.FIXED_INT_64) {
                return new UncompressedLongBlockCursor(positionCount, slice);
            }
            if (type == Type.DOUBLE) {
                return new UncompressedDoubleBlockCursor(positionCount, slice);
            }
            if (type == Type.VARIABLE_BINARY) {
                return new UncompressedSliceBlockCursor(positionCount, slice);
            }
        }
        return new UncompressedBlockCursor(tupleInfo, positionCount, slice);
    }

    @Override
    public UncompressedBlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(tupleInfo);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("tupleInfo", tupleInfo)
                .add("slice", slice)
                .toString();
    }
}
