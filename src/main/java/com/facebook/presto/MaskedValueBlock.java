/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.UncompressedPositionBlock.UncompressedPositionBlockCursor;
import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;

import java.util.List;

public class MaskedValueBlock implements ValueBlock
{
    private final ValueBlock valueBlock;
    private final List<Long> validPositions;

    public MaskedValueBlock(ValueBlock valueBlock, List<Long> validPositions)
    {
        this.valueBlock = valueBlock;
        this.validPositions = validPositions;
    }

    @Override
    public int getCount()
    {
        return validPositions.size();
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
        return false;
    }

    @Override
    public Range getRange()
    {
        return valueBlock.getRange();
    }

    @Override
    public BlockCursor blockCursor()
    {
        return new MaskedBlockCursor(valueBlock, validPositions);
    }

    private static class MaskedBlockCursor implements BlockCursor
    {
        private final BlockCursor valueCursor;
        private final BlockCursor validPositions;
        private boolean isValid;

        private MaskedBlockCursor(ValueBlock valueBlock, List<Long> validPositions)
        {
            this.validPositions = new UncompressedPositionBlockCursor(validPositions, valueBlock.getRange());
            this.valueCursor = valueBlock.blockCursor();
        }

        @Override
        public Range getRange()
        {
            return valueCursor.getRange();
        }

        @Override
        public boolean advanceToNextValue()
        {
            if (!isValid) {
                // advance to first position
                isValid = true;
                if (!validPositions.advanceNextPosition()) {
                    return false;
                }
            } else {
                // advance until the next position is after current value end position
                long currentValueEndPosition = valueCursor.getValuePositionEnd();
                while (validPositions.advanceNextPosition() && currentValueEndPosition <= validPositions.getPosition());
                if (currentValueEndPosition <= validPositions.getPosition()) {
                    return false;
                }
            }

            // move value cursor to to next position
            return valueCursor.advanceToPosition(validPositions.getPosition());
        }

        @Override
        public boolean advanceNextPosition()
        {
            // advance current position
            isValid = true;
            return validPositions.advanceNextPosition() &&
                    valueCursor.advanceToPosition(validPositions.getPosition());
        }

        @Override
        public boolean advanceToPosition(long newPosition)
        {
            isValid = true;

            return validPositions.advanceToPosition(newPosition) &&
                    valueCursor.advanceToPosition(validPositions.getPosition());
        }

        @Override
        public Tuple getTuple()
        {
            return valueCursor.getTuple();
        }

        @Override
        public long getLong(int field)
        {
            return valueCursor.getLong(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return valueCursor.getSlice(field);
        }

        @Override
        public long getPosition()
        {
            return valueCursor.getPosition();
        }

        @Override
        public long getValuePositionEnd()
        {
            return valueCursor.getValuePositionEnd();
        }

        @Override
        public boolean tupleEquals(Tuple value)
        {
            return valueCursor.tupleEquals(value);
        }
    }
}
