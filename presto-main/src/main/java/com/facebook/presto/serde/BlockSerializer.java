package com.facebook.presto.serde;

import com.facebook.presto.slice.SliceOutput;

public interface BlockSerializer
{
    /**
     * Create a TupleStreamWriter that can be used to serialize one or more TupleStreams together
     */
    BlocksWriter createBlockWriter(SliceOutput sliceOutput);
}
