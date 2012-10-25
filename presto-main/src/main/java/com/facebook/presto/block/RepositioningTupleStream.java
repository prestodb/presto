package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RepositioningTupleStream
        implements TupleStream
{
    private final TupleStream delegate;
    private long startingPosition;

    public RepositioningTupleStream(TupleStream delegate, long startingPosition)
    {
        checkNotNull(delegate, "delegate is null");
        checkArgument(startingPosition >= 0, "startingPosition must be at least zero");

        this.delegate = delegate;
        this.startingPosition = startingPosition;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return delegate.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        checkNotNull(session, "session is null");
        return new RepositioningCursor(delegate.cursor(session), startingPosition);
    }

    private static class RepositioningCursor
            extends ForwardingCursor
    {
        private long newStreamPosition;
        private long valuePositionLength;

        private RepositioningCursor(Cursor cursor, long startingPosition)
        {
            super(Preconditions.checkNotNull(cursor, "cursor is null"));
            checkArgument(startingPosition >= 0, "startingPosition must be at least zero");
            // Initialize to some values that will work when we advance from the beginning
            newStreamPosition = startingPosition - 1;
            valuePositionLength = 1;
        }

        @Override
        public Range getRange()
        {
            return Range.ALL;
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            AdvanceResult advanceResult = getDelegate().advanceNextValue();
            if (advanceResult == AdvanceResult.SUCCESS) {
                newStreamPosition += valuePositionLength;
                valuePositionLength = getDelegate().getCurrentValueEndPosition() - getDelegate().getPosition() + 1;
            }
            return advanceResult;
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            AdvanceResult advanceResult = getDelegate().advanceNextPosition();
            if (advanceResult == AdvanceResult.SUCCESS) {
                newStreamPosition++;
                valuePositionLength = getDelegate().getCurrentValueEndPosition() - getDelegate().getPosition() + 1;
            }
            return advanceResult;
        }

        @Override
        public AdvanceResult advanceToPosition(long position)
        {
            return Cursors.advanceToPositionByValues(this, position);
        }

        @Override
        public long getPosition()
        {
            Cursors.checkReadablePosition(this);
            return newStreamPosition;
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            Cursors.checkReadablePosition(this);
            return newStreamPosition + (getDelegate().getCurrentValueEndPosition() - getDelegate().getPosition());
        }
    }
}
