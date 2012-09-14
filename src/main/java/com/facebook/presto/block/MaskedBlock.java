/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.position.UncompressedPositionBlock.UncompressedPositionBlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class MaskedBlock implements TupleStream
{
    private final TupleStream valueBlock;
    private final List<Long> validPositions;

    public MaskedBlock(TupleStream valueBlock, List<Long> validPositions)
    {
        Preconditions.checkNotNull(valueBlock, "valueBlock is null");
        Preconditions.checkNotNull(validPositions, "validPositions is null");
        Preconditions.checkArgument(!validPositions.isEmpty(), "validPositions is empty");

        this.valueBlock = valueBlock;
        this.validPositions = ImmutableList.copyOf(validPositions);
    }

    public int getCount()
    {
        return validPositions.size();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return valueBlock.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return valueBlock.getRange();
    }

    @Override
    public Cursor cursor()
    {
        return new MaskedBlockCursor(valueBlock, validPositions);
    }

    private static class MaskedBlockCursor implements Cursor
    {
        private final Cursor valueCursor;
        private final Cursor validPositions;
        private boolean isValid;

        private MaskedBlockCursor(TupleStream valueBlock, List<Long> validPositions)
        {
            this.validPositions = new UncompressedPositionBlockCursor(validPositions, valueBlock.getRange());
            this.valueCursor = valueBlock.cursor();
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return valueCursor.getTupleInfo();
        }

        @Override
        public Range getRange()
        {
            return valueCursor.getRange();
        }

        @Override
        public boolean isFinished()
        {
            return valueCursor.isFinished();
        }

        @Override
        public boolean advanceNextValue()
        {
            if (!isValid) {
                // advance to first position
                isValid = true;
                if (!validPositions.advanceNextPosition()) {
                    return false;
                }
            } else {
                // advance until the next position is after current value end position
                long currentValueEndPosition = valueCursor.getCurrentValueEndPosition();

                do {
                    if (!validPositions.advanceNextPosition()) {
                        return false;
                    }
                } while (validPositions.getPosition() <= currentValueEndPosition);
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
        public double getDouble(int field)
        {
            return valueCursor.getDouble(field);
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
        public long getCurrentValueEndPosition()
        {
            return valueCursor.getCurrentValueEndPosition();
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            return valueCursor.currentTupleEquals(value);
        }
    }
}
