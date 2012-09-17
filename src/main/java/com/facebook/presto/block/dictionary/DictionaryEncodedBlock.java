package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.slice.Slice;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class DictionaryEncodedBlock implements TupleStream
{
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final TupleStream sourceValueBlock;

    public DictionaryEncodedBlock(TupleInfo tupleInfo, Slice[] dictionary, TupleStream sourceValueBlock)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceValueBlock, "sourceValueBlock is null");

        this.tupleInfo = tupleInfo;
        this.dictionary = dictionary;
        this.sourceValueBlock = sourceValueBlock;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return sourceValueBlock.getRange();
    }

    @Override
    public Cursor cursor()
    {
        return new DictionaryEncodedBlockCursor(tupleInfo, sourceValueBlock, dictionary);
    }

    private static class DictionaryEncodedBlockCursor implements Cursor
    {
        private final TupleInfo tupleInfo;
        private final Cursor delegate;
        private final Slice[] dictionary;

        private DictionaryEncodedBlockCursor(TupleInfo tupleInfo, TupleStream sourceValueBlock, Slice... dictionary)
        {
            this.tupleInfo = tupleInfo;
            this.dictionary = dictionary;
            delegate = sourceValueBlock.cursor();
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
        public boolean isValid()
        {
            return delegate.isValid();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public boolean advanceNextValue()
        {
            return delegate.advanceNextValue();
        }

        @Override
        public boolean advanceNextPosition()
        {
            return delegate.advanceNextPosition();
        }

        @Override
        public boolean advanceToPosition(long position)
        {
            return delegate.advanceToPosition(position);
        }

        @Override
        public Tuple getTuple()
        {
            return new Tuple(getTupleSlice(), tupleInfo);
        }

        @Override
        public long getLong(int field)
        {
            return tupleInfo.getLong(getTupleSlice(), field);
        }

        @Override
        public double getDouble(int field)
        {
            return tupleInfo.getDouble(getTupleSlice(), field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return tupleInfo.getSlice(getTupleSlice(), field);
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            // todo We should be able to compare the dictionary keys directly if the tuple comes from a block encoded using the same dictionary
            return tupleInfo.equals(value.getTupleInfo()) && getTupleSlice().equals(value.getTupleSlice());
        }

        @Override
        public long getPosition()
        {
            return delegate.getPosition();
        }

        @Override
        public long getCurrentValueEndPosition()
        {
            return delegate.getCurrentValueEndPosition();
        }
        
        private Slice getTupleSlice()
        {
            int dictionaryKey = Ints.checkedCast(delegate.getLong(0));
            checkPositionIndex(dictionaryKey, dictionary.length, "dictionaryKey does not exist");
            return dictionary[dictionaryKey];
        }
    }
}
