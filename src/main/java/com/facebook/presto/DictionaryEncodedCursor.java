package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;

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
        // This should be very memory efficient since the returned cursor is just the cached dictionary value
        return decodeSliceValue(Ints.checkedCast(sourceCursor.getLong(0)));
    }

    @Override
    public long getPosition()
    {
        return sourceCursor.getPosition();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        checkNotNull(value, "value is null");
        return value.size() == 1 && getSlice(0).equals(value.getTupleSlice());
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

    private Slice decodeSliceValue(int dictionaryKey) {
        checkPositionIndex(dictionaryKey, dictionary.length, "Invalid dictionary key");
        return dictionary[dictionaryKey];
    }
}
