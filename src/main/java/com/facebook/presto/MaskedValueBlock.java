/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.NoSuchElementException;

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
        private BlockCursor valueCursor;
        private List<Long> validPositions;
        private int currentPositionIndex = -1;
        private int nextValueIndex = 0;

        private MaskedBlockCursor(ValueBlock valueBlock, List<Long> validPositions)
        {
            this.validPositions = validPositions;
            this.valueCursor = valueBlock.blockCursor();
        }

        @Override
        public Range getRange()
        {
            return valueCursor.getRange();
        }

        @Override
        public void moveTo(BlockCursor newPosition)
        {
            MaskedBlockCursor other = (MaskedBlockCursor) newPosition;
            this.valueCursor.moveTo(other.valueCursor);
            this.validPositions = other.validPositions;
            this.currentPositionIndex = other.currentPositionIndex;
            this.nextValueIndex = other.nextValueIndex;
        }

        @Override
        public boolean hasNextValue()
        {
            return nextValueIndex >= 0;
        }

        @Override
        public void advanceNextValue()
        {
            if (!hasNextValue()) {
                throw new NoSuchElementException();
            }

            // move current to next position
            currentPositionIndex = nextValueIndex;
            valueCursor.advanceToPosition(validPositions.get(currentPositionIndex));
            findNextValuePosition();
        }

        @Override
        public boolean hasNextPosition()
        {
            return currentPositionIndex < validPositions.size() - 1;
        }

        @Override
        public void advanceNextPosition()
        {
            if (!hasNextPosition()) {
                throw new NoSuchElementException();
            }

            // advance current position
            currentPositionIndex++;
            valueCursor.advanceToPosition(validPositions.get(currentPositionIndex));

            // if current position caught up to next value position, find a new next value position
            if (currentPositionIndex >= nextValueIndex) {
                findNextValuePosition();
            }
        }

        private void findNextValuePosition()
        {
            // advance next value index until it has a position after the current value end
            do {
                nextValueIndex++;
            } while (nextValueIndex < validPositions.size() && validPositions.get(nextValueIndex) < valueCursor.getValuePositionEnd());
            if (nextValueIndex == validPositions.size()) {
                nextValueIndex = -1;
            }
        }

        @Override
        public void advanceToPosition(long position)
        {
            // todo only advance of position is possible
            while (currentPositionIndex < validPositions.size() && validPositions.get(currentPositionIndex) < position) {
                currentPositionIndex++;
            }
            if (currentPositionIndex == validPositions.size()) {
                // no more data
                nextValueIndex = -1;
                throw new NoSuchElementException();
            }
            position = validPositions.get(currentPositionIndex);
            valueCursor.advanceToPosition(position);

            Preconditions.checkState(valueCursor.getPosition() == position, "Advanced to unexpected position");

            // if current position caught up to next value position, find a new next value position
            if (currentPositionIndex >= nextValueIndex) {
                findNextValuePosition();
            }
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
