package com.facebook.presto.operator.tap;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Feeds data to an TupleValueSink one value at a time (rather than one position at a time) 
 * For completeness, TupleValueSink used here should process the entire range for the given
 * positional value on each invocation.
 */
public class Tap
        implements TupleStream
{
    private final TupleStream tupleStream;
    private final TupleValueSink tupleValueSink;
    private boolean first = true;

    public Tap(TupleStream tupleStream, TupleValueSink tupleValueSink)
    {
        this.tupleStream = checkNotNull(tupleStream, "tupleStream is null");
        this.tupleValueSink = checkNotNull(tupleValueSink, "inlinedOperatorWriter is null");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleStream.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return tupleStream.getRange();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        if (first) {
            first = false;
            return new InterceptingCursor(tupleStream.cursor(session), tupleValueSink);
        }
        else {
            // TODO: consider making this work with multiple cursors using sessions
            throw new UnsupportedOperationException("only support single cursors right now");
        }
    }

    private static class InterceptingCursor
            extends ForwardingCursor
    {
        private final TupleValueSink tupleValueSink;
        private final TupleStreamPosition tupleStreamPosition;
        private long measuredPosition = -1;

        private InterceptingCursor(Cursor cursor, TupleValueSink tupleValueSink)
        {
            super(checkNotNull(cursor, "cursor is null"));
            this.tupleValueSink = checkNotNull(tupleValueSink, "inlinedOperatorWriter is null");
            tupleStreamPosition = Cursors.asTupleStreamPosition(cursor);
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            AdvanceResult result = getDelegate().advanceNextValue();
            processCurrentValueIfNecessary(result);
            return result;
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            AdvanceResult result = getDelegate().advanceNextPosition();
            processCurrentValueIfNecessary(result);
            return result;
        }

        @Override
        public AdvanceResult advanceToPosition(long position)
        {
            // We should always have processed as much as the current value end position
            while (position > getDelegate().getCurrentValueEndPosition()) {
                AdvanceResult result = getDelegate().advanceNextValue();
                processCurrentValueIfNecessary(result);
                if (result == AdvanceResult.MUST_YIELD || result == AdvanceResult.FINISHED) {
                    return result;
                }
            }
            // All intermediate values and all positions of the current value should already be processed
            return (position == getDelegate().getPosition()) ? AdvanceResult.SUCCESS : getDelegate().advanceToPosition(position);
        }

        private void processCurrentValueIfNecessary(AdvanceResult advanceResult)
        {
            switch (advanceResult) {
                case SUCCESS:
                    if (getDelegate().getPosition() > measuredPosition) {
                        tupleValueSink.process(tupleStreamPosition);
                        measuredPosition = getDelegate().getCurrentValueEndPosition();
                    }
                    break;
                case FINISHED:
                    tupleValueSink.finished();
                    break;
                case MUST_YIELD:
                    // Do nothing
                    break;
                default:
                    throw new AssertionError("unknown advance results");
            }
        }
    }
}
