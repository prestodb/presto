package com.facebook.presto.block;

import com.facebook.presto.slice.SliceOutput;

public interface TupleStreamSerializer
{
    /**
     * Create a TupleStreamWriter that can be used to serialize one or more TupleStreams together
     */
    TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput);
}
