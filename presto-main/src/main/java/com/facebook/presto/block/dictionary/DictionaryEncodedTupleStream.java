package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractYieldingIterator;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedTupleStream
        implements TupleStream, YieldingIterable<TupleStream>
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
    public YieldingIterator<TupleStream> iterator(QuerySession session)
    {
        final YieldingIterator<TupleStream> iterator = ((YieldingIterable<TupleStream>) sourceTupleStream).iterator(session);
        return new AbstractYieldingIterator<TupleStream>()
        {
            @Override
            protected TupleStream computeNext()
            {
                if (iterator.canAdvance()) {
                    return new DictionaryEncodedTupleStream(dictionary, iterator.next());
                }
                if (iterator.mustYield()) {
                    return setMustYield();
                }
                return endOfData();
            }
        };
    }

    @Override
    public DictionaryEncodedCursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new DictionaryEncodedCursor(dictionary, sourceTupleStream.cursor(session));
    }
}
