package com.facebook.presto;

import java.io.Closeable;

public interface RowSource
        extends BlockStream<ValueBlock>, Closeable
{
}
