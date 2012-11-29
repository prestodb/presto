package com.facebook.presto.block.rle;

import com.facebook.presto.block.Block;
import com.facebook.presto.serde.RunLengthBlockEncoding;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public class RunLengthEncodedBlock
        implements Block
{
    private final Tuple value;
    private final int positionCount;

    public RunLengthEncodedBlock(Tuple value, int positionCount)
    {
        this.value = value;
        this.positionCount = positionCount;
    }

    public Tuple getValue()
    {
        return value;
    }

    public Tuple getSingleValue()
    {
        return value;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(value.getTupleSlice().length(), Unit.BYTE);
    }

    @Override
    public RunLengthBlockEncoding getEncoding()
    {
        return new RunLengthBlockEncoding(value.getTupleInfo());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        Preconditions.checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return value.getTupleInfo();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("positionCount", positionCount)
                .toString();
    }

    @Override
    public RunLengthEncodedBlockCursor cursor()
    {
        return new RunLengthEncodedBlockCursor(value, positionCount);
    }

}
