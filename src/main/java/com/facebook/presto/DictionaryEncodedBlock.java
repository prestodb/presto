package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlock implements ValueBlock
{
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final ValueBlock sourceValueBlock;

    public DictionaryEncodedBlock(TupleInfo tupleInfo,  Slice[] dictionary, ValueBlock sourceValueBlock)
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

        private DictionaryEncodedBlockCursor(TupleInfo tupleInfo, ValueBlock sourceValueBlock, Slice... dictionary)
        {
            this.tupleInfo = tupleInfo;
            this.dictionary = dictionary;
            delegate = sourceValueBlock.blockCursor();
        }

        private DictionaryEncodedBlockCursor(TupleInfo tupleInfo, BlockCursor delegate, Slice[] dictionary)
        {
            this.tupleInfo = tupleInfo;
            this.delegate = delegate;
            this.dictionary = dictionary;
        }

        @Override
        public Range getRange()
        {
            return delegate.getRange();
        }

        @Override
        public BlockCursor duplicate()
        {
            return new DictionaryEncodedBlockCursor(tupleInfo, delegate.duplicate(), dictionary);
        }

        @Override
        public void moveTo(BlockCursor newPosition)
        {
            delegate.moveTo(((DictionaryEncodedBlockCursor)newPosition).delegate);
        }

        @Override
        public boolean hasNextValue()
        {
            return delegate.hasNextValue();
        }

        @Override
        public void advanceNextValue()
        {
            delegate.advanceNextValue();
        }

        @Override
        public boolean hasNextValuePosition()
        {
            return delegate.hasNextValuePosition();
        }

        @Override
        public void advanceNextValuePosition()
        {
            delegate.advanceNextValuePosition();
        }

        @Override
        public void advanceToPosition(long position)
        {
            delegate.advanceToPosition(position);
        }

        @Override
        public Tuple getTuple()
        {
            return new Tuple(getSlice(0), tupleInfo);
        }

        @Override
        public long getLong(int field)
        {
            return delegate.getLong(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            int dictionaryKey = Ints.checkedCast(getLong(0));
            Preconditions.checkPositionIndex(dictionaryKey, dictionary.length, "dictionaryKey does not exist");
            return dictionary[(int) getLong(0)];
        }

        @Override
        public boolean tupleEquals(Tuple value)
        {
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
