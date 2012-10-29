package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.slice.Slice;

public interface TupleStreamDeserializer
{
    /**
     * Extract the TupleStream that has been serialized to the Slice.
     * In most cases, this will provide a view on top of the specified Slice and assumes
     * that the contents of the underlying Slice will not be changing.
     */
    TupleStream deserialize(Range totalRange, Slice slice);
}
