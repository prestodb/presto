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
    private final Dictionary dictionary;
    private final TupleStream sourceValueBlock;

    public DictionaryEncodedBlock(Dictionary dictionary, TupleStream sourceValueBlock)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceValueBlock, "sourceValueBlock is null");

        this.dictionary = dictionary;
        this.sourceValueBlock = sourceValueBlock;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return sourceValueBlock.getRange();
    }

    @Override
    public Cursor cursor()
    {
        return new DictionaryEncodedBlockCursor(dictionary, sourceValueBlock);
    }

    private static class DictionaryEncodedBlockCursor implements Cursor
    {
        private final Dictionary dictionary;
        private final Cursor delegate;

        private DictionaryEncodedBlockCursor(Dictionary dictionary, TupleStream sourceValueBlock)
        {
            this.dictionary = dictionary;
            delegate = sourceValueBlock.cursor();
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return dictionary.getTupleInfo();
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
            return dictionary.getTuple(getDictionaryKey());
        }

        @Override
        public long getLong(int field)
        {
            return dictionary.getLong(getDictionaryKey(), field);
        }

        @Override
        public double getDouble(int field)
        {
            return dictionary.getDouble(getDictionaryKey(), field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return dictionary.getSlice(getDictionaryKey(), field);
        }

        @Override
        public boolean currentTupleEquals(Tuple value)
        {
            return dictionary.tupleEquals(getDictionaryKey(), value);
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
        
        public int getDictionaryKey()
        {
            int dictionaryKey = Ints.checkedCast(delegate.getLong(0));
            checkPositionIndex(dictionaryKey, dictionary.size(), "dictionaryKey does not exist");
            return dictionaryKey;
        }
    }
}
