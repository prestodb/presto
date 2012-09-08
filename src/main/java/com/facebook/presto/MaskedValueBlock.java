/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.base.Optional;

public class MaskedValueBlock implements ValueBlock
{
    public static Optional<ValueBlock> maskBlock(ValueBlock valueBlock, PositionBlock positions)
    {
//        if (!valueBlock.getRange().overlaps(positions.getRange())) {
//            return Optional.absent();
//        }
//
//        Range intersection = valueBlock.getRange().intersect(positions.getRange());
//
//        if (valueBlock.isSingleValue() && positions.isPositionsContiguous()) {
//            Tuple value = valueBlock.iterator().next();
//            return Optional.<ValueBlock>of(new RunLengthEncodedBlock(value, intersection));
//        }
//
//        Optional<PositionBlock> newPositionBlock;
//        if (valueBlock.isPositionsContiguous()) {
//            newPositionBlock = positions.filter(new RangePositionBlock(valueBlock.getRange()));
//        }
//        else {
//            newPositionBlock = positions.filter(valueBlock.toPositionBlock());
//        }
//
//        if (newPositionBlock.isPresent()) {
//            return Optional.<ValueBlock>of(new MaskedValueBlock(valueBlock, newPositionBlock.get()));
//        }

        return Optional.absent();
    }

    private final ValueBlock valueBlock;
    private final PositionBlock positionBlock;

    private MaskedValueBlock(ValueBlock valueBlock, PositionBlock positionBlock)
    {
        this.valueBlock = valueBlock;
        this.positionBlock = positionBlock;
    }

    @Override
    public int getCount()
    {
        return positionBlock.getCount();
    }

    @Override
    public boolean isSorted()
    {
        return valueBlock.isSorted();
    }

    @Override
    public boolean isSingleValue()
    {
        return valueBlock.isSingleValue();
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return positionBlock.isPositionsContiguous() && valueBlock.isPositionsContiguous();
    }

    @Override
    public Range getRange()
    {
        return positionBlock.getRange();
    }

    @Override
    public BlockCursor blockCursor()
    {
        throw new UnsupportedOperationException();
    }
}
