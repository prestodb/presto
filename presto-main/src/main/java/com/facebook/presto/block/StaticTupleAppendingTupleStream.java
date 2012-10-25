package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Appends a statically defined Tuple to the end of every Tuple produced by the underlying TupleStream
 */
public class StaticTupleAppendingTupleStream
        implements TupleStream
{
    private final TupleStream delegate;
    private final Tuple tuple;
    private final TupleInfo tupleInfo;

    public StaticTupleAppendingTupleStream(TupleStream delegate, Tuple tuple)
    {
        checkNotNull(delegate, "delegate is null");
        checkNotNull(tuple, "tuple is null");
        checkArgument(tuple.getTupleInfo().getFieldCount() > 0, "tuple should have at least one column");
        
        this.delegate = delegate;
        this.tuple = tuple;
        tupleInfo = new TupleInfo(
                ImmutableList.<TupleInfo.Type>builder()
                        .addAll(delegate.getTupleInfo().getTypes())
                        .addAll(tuple.getTupleInfo().getTypes())
                        .build()
        );
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return delegate.getRange();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        checkNotNull(session, "session is null");
        return new StaticTupleAppendingCursor(delegate.cursor(session), tuple);
    }

    private static class StaticTupleAppendingCursor
            extends ForwardingCursor
    {
        private final TupleInfo tupleInfo;
        private final Tuple tuple;

        private StaticTupleAppendingCursor(Cursor cursor, Tuple tuple)
        {
            super(checkNotNull(cursor, "cursor is null"));
            checkNotNull(tuple, "tuple is null");
            checkArgument(tuple.getTupleInfo().getFieldCount() > 0, "tuple should have at least one column");
            this.tuple = tuple;
            tupleInfo = new TupleInfo(
                    ImmutableList.<TupleInfo.Type>builder()
                            .addAll(cursor.getTupleInfo().getTypes())
                            .addAll(tuple.getTupleInfo().getTypes())
                            .build()
            );
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Tuple getTuple()
        {
            Cursors.checkReadablePosition(this);
            TupleInfo.Builder builder = tupleInfo.builder();
            Cursors.appendCurrentTupleToTupleBuilder(this, builder);
            return builder.build();
        }

        @Override
        public long getLong(int field)
        {
            Cursors.checkReadablePosition(this);
            int delegateFieldCount = getDelegate().getTupleInfo().getFieldCount();
            if (field < delegateFieldCount) {
                return getDelegate().getLong(field);
            }
            return tuple.getLong(field - delegateFieldCount);
        }

        @Override
        public double getDouble(int field)
        {
            Cursors.checkReadablePosition(this);
            int delegateFieldCount = getDelegate().getTupleInfo().getFieldCount();
            if (field < delegateFieldCount) {
                return getDelegate().getDouble(field);
            }
            return tuple.getDouble(field - delegateFieldCount);
        }

        @Override
        public Slice getSlice(int field)
        {
            Cursors.checkReadablePosition(this);
            int delegateFieldCount = getDelegate().getTupleInfo().getFieldCount();
            if (field < delegateFieldCount) {
                return getDelegate().getSlice(field);
            }
            return tuple.getSlice(field - delegateFieldCount);
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            checkNotNull(value, "value is null");
            Cursors.checkReadablePosition(this);
            return Cursors.currentTupleEquals(this, value);
        }
    }
}
