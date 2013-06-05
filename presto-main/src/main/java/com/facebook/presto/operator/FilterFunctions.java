package com.facebook.presto.operator;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.tuple.TupleReadable;

public class FilterFunctions
{
    public static final FilterFunction TRUE_FUNCTION = new TrueFilterFunction();

    private static class TrueFilterFunction
            implements FilterFunction
    {
        @Override
        public boolean filter(TupleReadable... cursors)
        {
            return true;
        }

        @Override
        public boolean filter(RecordCursor cursor)
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "TRUE";
        }
    }
}
