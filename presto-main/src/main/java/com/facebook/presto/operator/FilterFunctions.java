package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleReadable;

public class FilterFunctions
{
    public static final FilterFunction TRUE_FUNCTION = new FilterFunction()
    {
        @Override
        public boolean filter(TupleReadable... cursors)
        {
            return true;
        }
    };
}
