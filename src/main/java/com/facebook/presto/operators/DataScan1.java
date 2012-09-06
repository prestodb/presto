package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class DataScan1
        implements BlockStream<ValueBlock>
{
    private final BlockStream<?> source;
    private final Predicate<Cursor> predicate;

    public DataScan1(BlockStream<?> source, Predicate<Cursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        // todo maybe have a position block
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor cursor()
    {
        return new FilteredPositionCursor(source.cursor(), predicate);
    }

    private static class FilteredPositionCursor implements Cursor
    {
        private final Cursor cursor;
        private final Predicate<Cursor> predicate;
        private long currentPosition = -1;
        private long nextPosition = -1;

        public FilteredPositionCursor(Cursor cursor, Predicate<Cursor> predicate)
        {
            this.predicate = predicate;
            this.cursor = cursor;
            moveToNextPosition();
        }

        @Override
        public long getPosition()
        {
            Preconditions.checkState(currentPosition < 0, "Need to call advanceNext() first");
            return currentPosition;
        }

        @Override
        public boolean hasNextPosition()
        {
            return nextPosition >= 0;
        }

        @Override
        public void advanceNextPosition()
        {
            if (!hasNextPosition()) {
                throw new NoSuchElementException();
            }

            currentPosition = nextPosition;

            moveToNextPosition();
        }

        private void moveToNextPosition()
        {
            // todo this can be smarter about RLE blocks
            // advance to next valid position
            nextPosition = -1;
            while (cursor.hasNextPosition()) {
                cursor.advanceNextPosition();
                if (predicate.apply(cursor)) {
                    nextPosition = cursor.getPosition();
                    break;
                }
            }
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNextValue()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceNextValue()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Tuple getTuple()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice getSlice(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long peekNextValuePosition()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean currentValueEquals(Tuple value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextValueEquals(Tuple value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void advanceToPosition(long position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            throw new UnsupportedOperationException();
        }
    }
}
