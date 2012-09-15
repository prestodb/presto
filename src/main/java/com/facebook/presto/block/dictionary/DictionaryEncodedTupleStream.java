package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedTupleStream
        implements TupleStream
{
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final TupleStream sourceTupleStream;

    public DictionaryEncodedTupleStream(TupleInfo tupleInfo, Slice[] dictionary, TupleStream sourceTupleStream)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceTupleStream, "sourceTupleStream is null");
        checkArgument(tupleInfo.getFieldCount() == 1, "tupleInfo should only have one column");

        this.tupleInfo = tupleInfo;
        this.dictionary = dictionary;
        this.sourceTupleStream = sourceTupleStream;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor()
    {
        return new DictionaryEncodedCursor(tupleInfo, dictionary, sourceTupleStream.cursor());
    }
}
