package com.facebook.presto.operation;

import com.facebook.presto.block.Cursor;

public class DoubleLessThanComparison
    implements ComparisonOperation
{
    @Override
    public boolean evaluate(Cursor left, Cursor right)
    {
        return left.getDouble(0) < right.getDouble(0);
    }
}
