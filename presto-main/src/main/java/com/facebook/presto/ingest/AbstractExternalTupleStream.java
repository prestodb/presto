package com.facebook.presto.ingest;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractExternalTupleStream
        implements TupleStream
{
    protected final TupleInfo tupleInfo;
    private boolean cursorCreated;

    protected AbstractExternalTupleStream(TupleInfo tupleInfo)
    {
        this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        checkState(!cursorCreated, "Can only create one cursor from this stream");
        cursorCreated = true;
        return new ExternalCursor();
    }

    protected abstract boolean computeNext();

    protected abstract void buildTuple(TupleInfo.Builder tupleBuilder);

    protected abstract long getLong(int field);

    protected abstract double getDouble(int field);

    protected abstract Slice getSlice(int field);

    private class ExternalCursor
            implements Cursor
    {
        private long currentPosition = -1;
        private boolean finished;

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Range getRange()
        {
            return Range.ALL;
        }

        @Override
        public boolean isValid()
        {
            return currentPosition >= 0 && !isFinished();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public AdvanceResult advanceNextValue()
        {
            return advanceNextPosition();
        }

        @Override
        public AdvanceResult advanceNextPosition()
        {
            if (!computeNext()) {
                finished = true;
                return AdvanceResult.FINISHED;
            }
            currentPosition++;
            return AdvanceResult.SUCCESS;
        }

        @Override
        public AdvanceResult advanceToPosition(long position)
        {
            checkArgument(position >= 0, "position must be greater than or equal to zero");
            checkArgument(position >= currentPosition, "cannot advance backwards");
            while (currentPosition < position) {
                AdvanceResult result = advanceNextPosition();
                if (result != AdvanceResult.SUCCESS) {
                    return result;
                }
            }
            return AdvanceResult.SUCCESS;
        }

        @Override
        public Tuple getTuple()
        {
            Cursors.checkReadablePosition(this);
            TupleInfo.Builder tupleBuilder = tupleInfo.builder();
            buildTuple(tupleBuilder);
            checkState(tupleBuilder.isComplete(), "tuple is incomplete");
            return tupleBuilder.build();
        }

        @Override
        public long getLong(int field)
        {
            Cursors.checkReadablePosition(this);
            checkArgument(tupleInfo.getTypes().get(field) == TupleInfo.Type.FIXED_INT_64, "incorrect type");
            return AbstractExternalTupleStream.this.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            Cursors.checkReadablePosition(this);
            checkArgument(tupleInfo.getTypes().get(field) == TupleInfo.Type.DOUBLE, "incorrect type");
            return AbstractExternalTupleStream.this.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            Cursors.checkReadablePosition(this);
            checkArgument(tupleInfo.getTypes().get(field) == TupleInfo.Type.VARIABLE_BINARY, "incorrect type");
            return AbstractExternalTupleStream.this.getSlice(field);
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
            return currentPosition;
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            Cursors.checkReadablePosition(this);
            checkNotNull(value, "value is null");
            return Cursors.currentTupleEquals(this, value);
        }
    }
}
