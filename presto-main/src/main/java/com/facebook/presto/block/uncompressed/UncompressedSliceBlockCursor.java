package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class UncompressedSliceBlockCursor
        implements BlockCursor
{
    private final Slice slice;
    private final int positionCount;

    private int position;
    private int offset;
    private int size;

    public UncompressedSliceBlockCursor(int positionCount, Slice slice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(positionCount, "positionCount is null");

        this.positionCount = positionCount;

        this.slice = slice;
        offset = 0;
        size = 0;

        // start one position before the start
        position = -1;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return SINGLE_VARBINARY;
    }

    @Override
    public int getRemainingPositions()
    {
        return positionCount - (position + 1);
    }

    @Override
    public boolean isValid()
    {
        return 0 <= position && position < positionCount;
    }

    @Override
    public boolean isFinished()
    {
        return position >= positionCount;
    }

    private void checkReadablePosition()
    {
        Preconditions.checkState(isValid(), "cursor is not valid");
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= positionCount -1) {
            position = positionCount;
            return false;
        }

        position++;
        offset += size;
        size = slice.getInt(offset + SIZE_OF_BYTE);
        return true;
    }

    @Override
    public boolean advanceToPosition(int newPosition)
    {
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = slice.getInt(offset + SIZE_OF_BYTE);
        }
        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        // view port starts at next position
        int startOffset = offset + size;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        for (int i = 0; i < length; i++) {
            position++;
            offset += size;
            size = slice.getInt(offset + SIZE_OF_BYTE);
        }

        Slice newSlice = slice.slice(startOffset, offset + size - startOffset);
        return new UncompressedBlock(length, SINGLE_VARBINARY, newSlice);
    }

    @Override
    public int getPosition()
    {
        checkReadablePosition();
        return position;
    }

    @Override
    public Tuple getTuple()
    {
        checkReadablePosition();
        return new Tuple(slice.slice(offset, size), SINGLE_VARBINARY);
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
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.slice(offset + SIZE_OF_INT + SIZE_OF_BYTE, size - SIZE_OF_INT - SIZE_OF_BYTE);
    }

    @Override
    public boolean isNull(int field)
    {
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.getByte(offset) != 0;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }

    @Override
    public int getRawOffset()
    {
        return offset;
    }

    @Override
    public Slice getRawSlice()
    {
        return slice;
    }

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        blockBuilder.appendTuple(slice, offset, size);
    }
}
