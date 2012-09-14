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
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final Cursor sourceCursor;

    public DictionaryEncodedCursor(TupleInfo tupleInfo, Slice[] dictionary, Cursor sourceCursor)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceCursor, "sourceCursor is null");
        checkArgument(tupleInfo.getFieldCount() == 1, "tupleInfo should only have one column");

        this.tupleInfo = tupleInfo;
        this.dictionary = dictionary;
        this.sourceCursor = sourceCursor;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
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
    public boolean advanceNextValue()
    {
        return sourceCursor.advanceNextValue();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return sourceCursor.advanceNextPosition();
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
        int dictionaryKey = Ints.checkedCast(sourceCursor.getLong(0));
        checkPositionIndex(dictionaryKey, dictionary.length, "dictionaryKey does not exist");
        return dictionary[dictionaryKey];
    }

    @Override
    public long getPosition()
    {
        return sourceCursor.getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkNotNull(value, "value is null");
        return tupleInfo.equals(value.getTupleInfo()) && getSlice(0).equals(value.getTupleSlice());
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        return sourceCursor.advanceToPosition(position);
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return sourceCursor.getCurrentValueEndPosition();
    }
}
