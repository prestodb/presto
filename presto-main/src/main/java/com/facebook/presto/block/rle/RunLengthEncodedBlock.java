package com.facebook.presto.block.rle;

import com.facebook.presto.block.Block;
import com.facebook.presto.serde.RunLengthBlockEncoding;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class RunLengthEncodedBlock
        implements Block
{
    private final Tuple value;
    private final int positionCount;
    private final int rawPositionCount;

    public RunLengthEncodedBlock(Tuple value, int positionCount)
    {
        this.value = value;
        this.positionCount = positionCount;
        this.rawPositionCount = positionCount;
    }

    public RunLengthEncodedBlock(Tuple value, int positionCount, int rawPositionCount)
    {
        this.value = value;
        this.positionCount = positionCount;
        this.rawPositionCount = rawPositionCount;
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
    public RunLengthBlockEncoding getEncoding()
    {
        return new RunLengthBlockEncoding(value.getTupleInfo());
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
        return new RunLengthEncodedBlock(value, length, rawPositionCount);
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
