package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class TupleStreams
{
    public static Function<TupleStream, Cursor> getCursorFunction(final QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new Function<TupleStream, Cursor>()
        {
            @Override
            public Cursor apply(TupleStream input)
            {
                return input.cursor(session);
            }
        };
    }

    public static Function<TupleStream, Range> getRangeFunction()
    {
        return new Function<TupleStream, Range>()
        {
            @Override
            public Range apply(TupleStream input)
            {
                return input.getRange();
            }
        };
    }

}
