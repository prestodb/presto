package com.facebook.presto.block;

public interface TupleStreamWriter
{
    /**
     * Appends the specified TupleStream to this serialization
     */
    TupleStreamWriter append(TupleStream tupleStream);

    /**
     * Must be called after all TupleStreams have been appended to complete the serialization
     */
    void finish();
}
