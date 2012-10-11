package com.facebook.presto.operator.inlined;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.slice.Slice;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * All cursors produced from this TupleStream will return MUST_YIELD until the InlinedOperatorReader is finished
 */
public class FullInlinedOperatorTupleStream
        implements TupleStream
{
    private final InlinedOperatorReader inlinedOperatorReader;
    private TupleStream resultTupleStream;

    public FullInlinedOperatorTupleStream(InlinedOperatorReader inlinedOperatorReader)
    {
        this.inlinedOperatorReader = checkNotNull(inlinedOperatorReader, "inlinedOperatorReader is null");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return inlinedOperatorReader.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return inlinedOperatorReader.getRange();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        return new FullInlinedOperatorCursor(session);
    }

    private @Nullable Cursor createFullCursor(QuerySession session)
    {
        if (!inlinedOperatorReader.isFinished()) {
            return null;
        }
        if (resultTupleStream == null) {
            resultTupleStream = inlinedOperatorReader.getResult();
        }
        return resultTupleStream.cursor(session);
    }

    private class FullInlinedOperatorCursor
            implements Cursor
    {
        private final QuerySession querySession;
        private Cursor fullStreamCursor;

        private FullInlinedOperatorCursor(QuerySession querySession)
        {
            this.querySession = checkNotNull(querySession, "querySession is null");
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            if (fullStreamCursor == null) {
                fullStreamCursor = createFullCursor(querySession);
            }
            return (fullStreamCursor == null) ? AdvanceResult.MUST_YIELD : fullStreamCursor.advanceNextValue();
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            if (fullStreamCursor == null) {
                fullStreamCursor = createFullCursor(querySession);
            }
            return (fullStreamCursor == null) ? AdvanceResult.MUST_YIELD : fullStreamCursor.advanceNextPosition();
        }

        @Override
        public AdvanceResult advanceToPosition(long position)
        {
            if (fullStreamCursor == null) {
                fullStreamCursor = createFullCursor(querySession);
            }
            return (fullStreamCursor == null) ? AdvanceResult.MUST_YIELD : fullStreamCursor.advanceToPosition(position);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return inlinedOperatorReader.getTupleInfo();
        }

        @Override
        public Range getRange()
        {
            return inlinedOperatorReader.getRange();
        }

        @Override
        public boolean isValid()
        {
            return fullStreamCursor != null && fullStreamCursor.isValid();
        }

        @Override
        public boolean isFinished()
        {
            return fullStreamCursor != null && fullStreamCursor.isFinished();
        }

        @Override
        public Tuple getTuple()
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.getTuple();
        }

        @Override
        public long getLong(int field)
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.getSlice(field);
        }

        @Override
        public long getPosition()
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.getPosition();
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.getCurrentValueEndPosition();
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            if (fullStreamCursor == null) {
                throw new IllegalStateException("not yet advanced to a position");
            }
            return fullStreamCursor.currentTupleEquals(value);
        }
    }
}
