package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.*;

public class DictionaryEncodedCursor implements Cursor
{
    private final Dictionary dictionary;
    private final Cursor sourceCursor;

    public DictionaryEncodedCursor(Dictionary dictionary, Cursor sourceCursor)
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
    public Range getRange()
    {
        return sourceCursor.getRange();
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
    public AdvanceResult advanceNextValue()
    {
        return sourceCursor.advanceNextValue();
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        return sourceCursor.advanceNextPosition();
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
    public long getPosition()
    {
        return sourceCursor.getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        return dictionary.tupleEquals(getDictionaryKey(), value);
    }

    @Override
    public AdvanceResult advanceToPosition(long position)
    {
        return sourceCursor.advanceToPosition(position);
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return sourceCursor.getCurrentValueEndPosition();
    }

    public int getDictionaryKey()
    {
        int dictionaryKey = Ints.checkedCast(sourceCursor.getLong(0));
        checkPositionIndex(dictionaryKey, dictionary.size(), "dictionaryKey does not exist");
        return dictionaryKey;
    }
}
