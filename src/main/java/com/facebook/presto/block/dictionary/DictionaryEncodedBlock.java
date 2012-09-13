package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class DictionaryEncodedBlock implements Block
{
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final Block sourceValueBlock;

    public DictionaryEncodedBlock(TupleInfo tupleInfo, Slice[] dictionary, Block sourceValueBlock)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceValueBlock, "sourceValueBlock is null");
        checkArgument(tupleInfo.getFieldCount() == 1, "tupleInfo should only have one column");
        this.tupleInfo = tupleInfo;
        this.dictionary = dictionary;
        this.sourceValueBlock = sourceValueBlock;
    }

    @Override
    public int getCount()
    {
        return sourceValueBlock.getCount();
    }

    @Override
    public boolean isSorted()
    {
        return false;
    }

    @Override
    public boolean isSingleValue()
    {
        return sourceValueBlock.isSingleValue();
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return sourceValueBlock.isPositionsContiguous();
    }

    @Override
    public Range getRange()
    {
        return sourceValueBlock.getRange();
    }

    @Override
    public BlockCursor blockCursor()
    {
        return new DictionaryEncodedBlockCursor(tupleInfo, sourceValueBlock, dictionary);
    }

    private static class DictionaryEncodedBlockCursor implements BlockCursor
    {
        private final TupleInfo tupleInfo;
        private final BlockCursor delegate;
        private final Slice[] dictionary;

        private DictionaryEncodedBlockCursor(TupleInfo tupleInfo, Block sourceValueBlock, Slice... dictionary)
        {
            this.tupleInfo = tupleInfo;
            this.dictionary = dictionary;
            delegate = sourceValueBlock.blockCursor();
        }

        @Override
        public Range getRange()
        {
            return delegate.getRange();
        }

        @Override
        public boolean advanceToNextValue()
        {
            return delegate.advanceToNextValue();
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
            return new Tuple(getSlice(0), tupleInfo);
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field == 0, "should only have one field");
            return tupleInfo.getLong(getSlice(0), 0);
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field == 0, "should only have one field");
            return tupleInfo.getDouble(getSlice(0), 0);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field == 0, "should only have one field");
            int dictionaryKey = Ints.checkedCast(delegate.getLong(0));
            checkPositionIndex(dictionaryKey, dictionary.length, "dictionaryKey does not exist");
            return dictionary[dictionaryKey];
        }

        @Override
        public boolean tupleEquals(Tuple value)
        {
            // todo We should be able to compare the dictionary keys directly if the tuple comes from a block encoded using the same dictionary
            return tupleInfo.equals(value.getTupleInfo()) && getSlice(0).equals(value.getTupleSlice());
        }

        @Override
        public long getPosition()
        {
            return delegate.getPosition();
        }

        @Override
        public long getValuePositionEnd()
        {
            return delegate.getValuePositionEnd();
        }
    }
}
