package com.facebook.presto.block;

import com.facebook.presto.slice.Slice;

public interface TupleStreamDeserializer
{
    /**
     * Extract the TupleStream that has been serialized to the Slice
     */
    TupleStream deserialize(Slice slice);
}
