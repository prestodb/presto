package com.facebook.presto.block.dictionary;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class DictionaryEncodedBlockCursor implements BlockCursor
{
    private final Dictionary dictionary;
    private final BlockCursor sourceCursor;

    public DictionaryEncodedBlockCursor(Dictionary dictionary, BlockCursor sourceCursor)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceCursor, "sourceCursor is null");

        this.dictionary = dictionary;
        this.sourceCursor = sourceCursor;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    @Override
    public int getRemainingPositions()
    {
        return sourceCursor.getRemainingPositions();
    }

    @Override
    public boolean isValid()
    {
        return sourceCursor.isValid();
    }

    @Override
    public boolean isFinished()
    {
        return sourceCursor.isFinished();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return sourceCursor.advanceNextPosition();
    }

    @Override
    public boolean advanceToPosition(int position)
    {
        return sourceCursor.advanceToPosition(position);
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        return new DictionaryEncodedBlock(dictionary, sourceCursor.getRegionAndAdvance(length));
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
    public boolean isNull(int field)
    {
        return dictionary.isNull(getDictionaryKey(), field);
    }

    @Override
    public int getPosition()
    {
        return sourceCursor.getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        return dictionary.tupleEquals(getDictionaryKey(), value);
    }

    @Override
    public int getRawOffset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getRawSlice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        dictionary.appendTupleTo(getDictionaryKey(), blockBuilder);
    }

    public int getDictionaryKey()
    {
        int dictionaryKey = Ints.checkedCast(sourceCursor.getLong(0));
        checkPositionIndex(dictionaryKey, dictionary.size(), "dictionaryKey does not exist");
        return dictionaryKey;
    }
}
