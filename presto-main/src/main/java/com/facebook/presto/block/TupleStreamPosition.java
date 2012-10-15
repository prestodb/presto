package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

/**
 * Class that provides a positional view of a TupleStream that is backed by a Cursor.
 * Changes to the underlying cursor will also be reflected by this view.
 */
public class TupleStreamPosition
{
    private final Cursor cursor;

    public TupleStreamPosition(Cursor cursor)
    {
        Preconditions.checkNotNull(cursor, "cursor is null");
        Preconditions.checkArgument(cursor.isValid(), "cursor needs to be valid for a position");
        this.cursor = cursor;
    }

    public TupleInfo getTupleInfo()
    {
        return cursor.getTupleInfo();
    }

    public Range getRange()
    {
        return cursor.getRange();
    }

    public Tuple getTuple()
    {
        return cursor.getTuple();
    }

    public long getLong(int field)
    {
        return cursor.getLong(field);
    }

    public double getDouble(int field)
    {
        return cursor.getDouble(field);
    }

    public Slice getSlice(int field)
    {
        return cursor.getSlice(field);
    }

    public long getPosition()
    {
        return cursor.getPosition();
    }

    public long getCurrentValueEndPosition()
    {
        return cursor.getCurrentValueEndPosition();
    }

    public boolean currentTupleEquals(Tuple value)
    {
        return cursor.currentTupleEquals(value);
    }

    /**
     * Get the current TupleStreamPosition as a TupleStream facade with one value.
     * Changes to the underlying cursor will be reflected within this tuple stream as well.
     * However, any changes to the underlying cursor will IMMEDIATELY make any cursors created from
     * the TupleStreamAdapter to become invalid and should be discarded.
     */
    public TupleStream asBoundedTupleStream()
    {
        return new TupleStreamAdapter(cursor);
    }

    private static class TupleStreamAdapter
            implements TupleStream
    {
        private final Cursor cursor;

        private TupleStreamAdapter(Cursor cursor)
        {
            this.cursor = Preconditions.checkNotNull(cursor, "cursor is null");
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return cursor.getTupleInfo();
        }

        @Override
        public Range getRange()
        {
            return Range.create(cursor.getPosition(), cursor.getCurrentValueEndPosition());
        }

        @Override
        public Cursor cursor(QuerySession session)
        {
            return new CurrentValueCursor(cursor);
        }

        private static class CurrentValueCursor
            extends ForwardingCursor
        {
            private final Range range;
            private long currentPosition;

            private CurrentValueCursor(Cursor cursor)
            {
                super(cursor);
                currentPosition = cursor.getPosition() - 1;
                range = Range.create(cursor.getPosition(), cursor.getCurrentValueEndPosition());
            }

            @Override
            public boolean isValid()
            {
                return range.contains(currentPosition);
            }

            @Override
            public boolean isFinished()
            {
                return currentPosition > range.getEnd();
            }

            @Override
            public AdvanceResult advanceNextValue()
            {
                if (currentPosition < range.getStart()) {
                    // Not yet initialized
                    currentPosition = range.getStart();
                    return AdvanceResult.SUCCESS;
                }
                else {
                    // Should only have one unique value
                    currentPosition = range.getEnd() + 1;
                    return AdvanceResult.FINISHED;
                }
            }

            @Override
            public AdvanceResult advanceNextPosition()
            {
                currentPosition++;
                return isFinished() ? AdvanceResult.FINISHED : AdvanceResult.SUCCESS;
            }

            @Override
            public AdvanceResult advanceToPosition(long position)
            {
                Preconditions.checkArgument(position >= currentPosition, "position needs to advance forward");
                currentPosition = position;
                return isFinished() ? AdvanceResult.FINISHED : AdvanceResult.SUCCESS;
            }

            @Override
            public long getPosition()
            {
                Cursors.checkReadablePosition(this);
                return currentPosition;
            }

            @Override
            public long getCurrentValueEndPosition()
            {
                Cursors.checkReadablePosition(this);
                return range.getEnd();
            }
        }
    }
}
