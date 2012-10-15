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

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.MUST_YIELD;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

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
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new MaskedBlockCursor(valueBlock.cursor(session), validPositions);
    }

    private static class MaskedBlockCursor implements Cursor
    {
        private final Cursor valueCursor;
        private final Cursor validPositions;
        private boolean isValid;

        private MaskedBlockCursor(Cursor cursor, List<Long> validPositions)
        {
            this.validPositions = new UncompressedPositionBlockCursor(validPositions, cursor.getRange());
            this.valueCursor = cursor;
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
        public boolean isValid()
        {
            return valueCursor.isValid();
        }

        @Override
        public boolean isFinished()
        {
            return valueCursor.isFinished();
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            if (isFinished()) {
                return FINISHED;
            }

            if (!isValid) {
                // advance to first position
                isValid = true;
                AdvanceResult result = validPositions.advanceNextPosition();
                if (result == MUST_YIELD) {
                    // advance the value cursor before yielding
                    valueCursor.advanceToPosition(validPositions.getPosition());
                    return MUST_YIELD;
                }
                if (result == FINISHED) {
                    // move to end of value cursor
                    // todo replace with cursor.close()
                    valueCursor.advanceToPosition(Long.MAX_VALUE);
                    return FINISHED;
                }
            } else {
                // advance until the next position is after current value end position
                long currentValueEndPosition = valueCursor.getCurrentValueEndPosition();

                do {
                    AdvanceResult result = validPositions.advanceNextPosition();
                    if (result == MUST_YIELD) {
                        // advance the value cursor before yielding
                        valueCursor.advanceToPosition(validPositions.getPosition());
                        return MUST_YIELD;
                    }
                    if (result == FINISHED) {
                        // move to end of value cursor
                        // todo replace with cursor.close()
                        valueCursor.advanceToPosition(Long.MAX_VALUE);
                        return FINISHED;
                    }
                } while (validPositions.getPosition() <= currentValueEndPosition);
            }

            // move value cursor to to next position
            AdvanceResult result = valueCursor.advanceToPosition(validPositions.getPosition());
            // todo forcing the advance to be successful is wrong: if the advance next position works, and we yield here, we will skip positions
            Preconditions.checkState(result == SUCCESS);
            return result;
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            if (isFinished()) {
                return FINISHED;
            }

            // advance current position
            isValid = true;
            AdvanceResult result = validPositions.advanceNextPosition();
            if (result == MUST_YIELD) {
                // advance the value cursor before yielding
                valueCursor.advanceToPosition(validPositions.getPosition());
                return MUST_YIELD;
            }
            if (result == FINISHED) {
                // move to end of value cursor
                // todo replace with cursor.close()
                valueCursor.advanceToPosition(Long.MAX_VALUE);
                return FINISHED;
            }
            result = valueCursor.advanceToPosition(validPositions.getPosition());
            // todo forcing the advance to be successful is wrong: if the advance next position works, and we yield here, we will skip positions
            Preconditions.checkState(result == SUCCESS);
            return SUCCESS;
        }

        @Override
        public AdvanceResult advanceToPosition(long newPosition)
        {
            if (isFinished()) {
                return FINISHED;
            }

            isValid = true;
            AdvanceResult result = validPositions.advanceToPosition(newPosition);
            if (result == MUST_YIELD) {
                // advance the value cursor before yielding
                valueCursor.advanceToPosition(validPositions.getPosition());
                return MUST_YIELD;
            }
            if (result == FINISHED) {
                // move to end of value cursor
                // todo replace with cursor.close()
                valueCursor.advanceToPosition(Long.MAX_VALUE);
                return FINISHED;
            }
            result = valueCursor.advanceToPosition(validPositions.getPosition());
            // todo forcing the advance to be successful is wrong: if the advance next position works, and we yield here, we will skip positions
            Preconditions.checkState(result == SUCCESS);
            return SUCCESS;
        }

        @Override
        public Tuple getTuple()
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.getTuple();
        }

        @Override
        public long getLong(int field)
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.getSlice(field);
        }

        @Override
        public long getPosition()
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.getPosition();
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.getCurrentValueEndPosition();
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            Cursors.checkReadablePosition(this);
            return valueCursor.currentTupleEquals(value);
        }
    }
}
