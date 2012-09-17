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
    public long getPosition()
    {
        return sourceCursor.getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkNotNull(value, "value is null");
        return tupleInfo.equals(value.getTupleInfo()) && getTupleSlice().equals(value.getTupleSlice());
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
    
    private Slice getTupleSlice()
    {
        int dictionaryKey = Ints.checkedCast(sourceCursor.getLong(0));
        checkPositionIndex(dictionaryKey, dictionary.length, "dictionaryKey does not exist");
        return dictionary[dictionaryKey];
    }
}
