package com.facebook.presto.block.rle;

import com.facebook.presto.util.Range;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.Block;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class RunLengthEncodedBlock
        implements Block
{
    private final Tuple value;
    private final Range range;
    private final Range rawRange;

    public RunLengthEncodedBlock(Tuple value, Range range)
    {
        this.value = value;
        this.range = range;
        this.rawRange = range;
    }

    private RunLengthEncodedBlock(Tuple value, Range range, Range rawRange)
    {
        this.value = value;
        this.range = range;
        this.rawRange = rawRange;
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
        return (int) (range.getEnd() - range.getStart() + 1);
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public Range getRawRange()
    {
        return rawRange;
    }

    @Override
    public Block createViewPort(Range viewPortRange)
    {
        Preconditions.checkArgument(rawRange.contains(viewPortRange), "view port range is must be within the range range of this block");
        return new RunLengthEncodedBlock(value, viewPortRange, rawRange);
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
                .add("range", range)
                .toString();
    }

    @Override
    public RunLengthEncodedBlockCursor cursor()
    {
        return new RunLengthEncodedBlockCursor(value, range);
    }

}
