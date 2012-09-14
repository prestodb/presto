package com.facebook.presto.ingest;

import com.facebook.presto.block.TupleStream;

import java.io.Closeable;

public interface RowSource
        extends TupleStream, Closeable
{
}
