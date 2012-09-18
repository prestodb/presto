package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;

import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedTupleStream
        implements TupleStream
{
    private final Dictionary dictionary;
    private final TupleStream sourceTupleStream;

    public DictionaryEncodedTupleStream(Dictionary dictionary, TupleStream sourceTupleStream)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceTupleStream, "sourceTupleStream is null");

        this.dictionary = dictionary;
        this.sourceTupleStream = sourceTupleStream;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    public Dictionary getDictionary()
    {
        return dictionary;
    }

    @Override
    public Range getRange()
    {
        return sourceTupleStream.getRange();
    }

    @Override
    public DictionaryEncodedCursor cursor()
    {
        return new DictionaryEncodedCursor(dictionary, sourceTupleStream.cursor());
    }
}
