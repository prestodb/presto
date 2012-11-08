package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;

public class FilterFunctions
{
    public static FilterFunction TRUE_FUNCTION = new FilterFunction()
    {
        @Override
        public boolean filter(BlockCursor[] cursors)
        {
            return true;
        }
    };
}
