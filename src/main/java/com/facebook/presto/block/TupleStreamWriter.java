package com.facebook.presto.block;

import java.io.Closeable;

public interface TupleStreamWriter
    extends Closeable
{
    /**
     * Appends the specified TupleStream to this serialization
     */
    TupleStreamWriter append(TupleStream tupleStream);

    /**
     * Must be called after all TupleStreams have been appended to complete the serialization
     */
    @Override
    void close();
}
