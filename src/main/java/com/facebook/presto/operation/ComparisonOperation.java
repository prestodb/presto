package com.facebook.presto.operation;

import com.facebook.presto.block.Cursor;

public interface ComparisonOperation
{
    boolean evaluate(Cursor left, Cursor right);
}
