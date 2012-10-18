package com.facebook.presto.operation;

import com.facebook.presto.block.Cursor;

public class LongLessThanComparison
    implements ComparisonOperation
{
    @Override
    public boolean evaluate(Cursor left, Cursor right)
    {
        return left.getLong(0) < right.getLong(0);
    }
}
